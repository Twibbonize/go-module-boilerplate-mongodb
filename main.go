package moduleboilerplate

import (
	"context"
	"encoding/json"
	"errors"
	"math/rand"
	"time"

	"github.com/Twibbonize/go-module-boilerplate-mongodb/types"
	"github.com/google/uuid"
	"github.com/redis/go-redis/v9"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/primitive"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

const (
	// Configurations
	LAST_UUID_TOTAL = 5
	DATA_PER_PAGE   = 45
	RAND_ID_LENGTH  = 16
	BLANK           = ""

	DAY_TTL            = 24 * time.Hour
	INDIVIDUAL_KEY_TTL = DAY_TTL * 7
	SORTED_SET_TTL     = DAY_TTL * 2

	// log signals
	ANYMODULE_CREATED = "anymodule-created"
	ANYMODULE_UPDATED = "anymodule-updated"
	ANYMODULE_DELETED = "anymodule-deleted"
)

var (

	// Any module errors
	UNAUTHORIZED = errors.New("Unauthorized!")
	NOT_FOUND    = errors.New(" not found on DB!")
	INVALID_UUID = errors.New("Invalid UUID!")

	// MongoDB errors
	MONGODB_FATAL_ERROR     = errors.New("MongoDB fatal error!")
	MONGODB_ERROR_DUPLICATE = errors.New("Error duplicate!")
	INVALID_HEX             = errors.New("Invalid object id hex format!")
	LAST_UUID_NOT_FOUND     = errors.New("Last UUID not found!")

	// Redis errors
	REDIS_FATAL_ERROR        = errors.New("Redis fatal error!")
	RANDID_KEY_NOT_FOUND     = errors.New("RandId key not found!")
	REDIS_KEY_NOT_FOUND      = errors.New("Redis key not found")
	INDIVIDUAL_KEY_NOT_FOUND = errors.New("Individual key not found")
	PARSE_JSON_FATAL_ERROR   = errors.New("Parse json fatal error!")
	JSON_MARSHAL_FATAL_ERROR = errors.New("JSON Marshalling fatal error!")

	// Pagination errors
	TOO_MUCH_LAST_UUID     = errors.New("Too much last UUID!")
	INSUFFICIENT_LAST_UUID = errors.New("Insufficient last UUID!")

	// Presign
	AWS_BUCKET_AUTH    = errors.New("Error authentication s3")
	AWS_BUCKET_PRESIGN = errors.New("Error creating presign s3")
)

func generateRandomString(length int) string {
	characters := "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789"
	result := make([]byte, length)

	for i := 0; i < length; i++ {
		result[i] = characters[rand.Intn(len(characters))]
	}

	return string(result)
}

func Init() (*types.Entity, *types.Error) {
	uuid := uuid.New().String()
	currentDate := time.Now().UnixMilli()

	return &types.Entity{
		ID:        primitive.NewObjectID(),
		UUID:      uuid,
		CreatedAt: currentDate,
		UpdatedAt: currentDate,
		RandID:    generateRandomString(16),
	}, nil
}

type SetterLib struct {
	anyModuleCollection *mongo.Collection
	redisClient         *redis.UniversalClient
	redis               CommonRedis
}

func NewSetterLib(
	anyModuleCollection *mongo.Collection,
	redis *redis.UniversalClient,
) *SetterLib {
	return &SetterLib{
		anyModuleCollection: anyModuleCollection,
		redisClient:         redis,
		redis: CommonRedis{
			client: redis,
		},
	}
}

// Create
//  1. Insert db
//  2. Set jsonstr to cache
//  3. Add to sorted set IF and only IF the sorted set exists
func (sl *SetterLib) Create(anyModule *types.Entity) *types.Error {
	if sl.anyModuleCollection == nil {
		return &types.Error{
			Err:     errors.New("anyModuleCollection is nil"),
			Message: "can't work with 42",
		}
	}

	//	1. Insert db
	sl.anyModuleCollection.InsertOne(context.TODO(), anyModule)

	//	2. Set jsonstr to cache
	err := sl.redis.Set(anyModule)
	if err != nil {
		return err
	}

	//	3. Add to sorted set
	total := sl.redis.TotalItemOnSortedSet(anyModule.AnyUUID)

	if total > 0 {
		err = sl.redis.SetSortedSet(anyModule)
		if err != nil {
			return err
		}
	}

	return nil
}

// Update
//  1. Update one db
//  2. Set to cache
func (sl *SetterLib) Update(anyModule *types.Entity) *types.Error {
	if sl.anyModuleCollection == nil {
		return &types.Error{
			Err:     errors.New("anyModuleCollection is nil"),
			Details: "The anyModuleCollection field is not initialized in SetterLib",
			Message: "Unable to update anyModule",
		}
	}

	filter := bson.M{"_id": anyModule.ID}
	update := bson.M{"$set": bson.M{
		"updatedat": time.Now().UnixMilli(),
		"randId":    anyModule.RandID,
	}}

	// 1. Update one db
	_, err := sl.anyModuleCollection.UpdateOne(context.TODO(), filter, update)
	if err != nil {
		return &types.Error{
			Err:     err,
			Details: "Failed to execute update on MongoDB",
			Message: "Update failed",
		}
	}

	//	2. Set to cache
	setErr := sl.redis.Set(anyModule)
	if setErr != nil {
		return setErr
	}

	return nil
}

// Delete
//  1. Delete one from db
//  2. Del jsonstr from cache
//  3. Remove item from sorted set
func (sl *SetterLib) Delete(anyModule *types.Entity) *types.Error {
	if sl.anyModuleCollection == nil {
		return &types.Error{
			Err:     errors.New("anyModuleCollection is nil"),
			Details: "The anyModuleCollection field is not initialized in SetterLib",
			Message: "Unable to delete anyModule",
		}
	}

	filter := bson.M{"_id": anyModule.ID}

	//	1. Delete one from db
	_, err := sl.anyModuleCollection.DeleteOne(context.TODO(), filter)
	if err != nil {
		return &types.Error{
			Err:     err,
			Details: "Failed to execute delete on MongoDB",
			Message: "Delete failed",
		}
	}

	//	2. Del jsonstr from cache
	sl.redis.Del(anyModule)

	//	3. Remove item from sorted set
	sl.redis.DeleteFromSortedSet(anyModule)

	return nil
}

// DeleteManyByAnyUUID
//  1. Find many by uuid => data
//  2. Loop data to delete all cache key
//  3. Delete sorted set key
//  4. Delete many by uuid from db
func (sl *SetterLib) DeleteManyByAnyUUID(anyUUID string) *types.Error {
	if sl.anyModuleCollection == nil {
		return &types.Error{
			Err:     errors.New("anyModuleCollection is nil"),
			Details: "The anyModuleCollection field is not initialized in SetterLib",
			Message: "Unable to delete module",
		}
	}

	filter := bson.M{"anyuuid": anyUUID}

	//	1. Find many by uuid => data
	cursor, errorFind := sl.anyModuleCollection.Find(
		context.TODO(),
		filter,
	)

	if errorFind != nil {
		return &types.Error{
			Err:     errorFind,
			Details: "Failed to execute find on MongoDB",
			Message: "Find many failed",
		}
	}

	defer cursor.Close(context.TODO())

	//	2. Loop data to delete all cache key
	for cursor.Next(context.TODO()) {
		var anyModule *types.Entity
		errorDecode := cursor.Decode(anyModule)

		if errorDecode != nil {
			continue
		}

		sl.redis.Del(anyModule)
	}

	//	3. Delete sorted set key
	sl.redis.DeleteSortedSet(anyUUID)

	//	4. Delete many by uuid from db
	_, err := sl.anyModuleCollection.DeleteMany(context.TODO(), filter)
	if err != nil {
		return &types.Error{
			Err:     err,
			Details: "Failed to execute delete on MongoDB",
			Message: "Delete many failed",
		}
	}

	return nil
}

func (sl *SetterLib) FindByUUID(uuid string) (*types.Entity, *types.Error) {
	if sl.anyModuleCollection == nil {
		return nil, &types.Error{
			Err:     MONGODB_FATAL_ERROR,
			Message: "Nil collection",
		}
	}

	var anyModuleDb *types.Entity
	filter := bson.M{"uuid": uuid}
	err := sl.anyModuleCollection.FindOne(context.TODO(), filter).Decode(anyModuleDb)
	if err != nil {
		return nil, &types.Error{
			Err:     MONGODB_FATAL_ERROR,
			Details: err.Error(),
			Message: "Find one failed",
		}
	}

	return anyModuleDb, nil
}

func (sl *SetterLib) FindByRandID(randid string) (*types.Entity, *types.Error) {
	if sl.anyModuleCollection == nil {
		return nil, &types.Error{
			Err:     MONGODB_FATAL_ERROR,
			Message: "Nil collection",
		}
	}

	//	1. Find one from db
	var anyModuleDb *types.Entity
	filter := bson.M{"randid": randid}
	err := sl.anyModuleCollection.FindOne(context.TODO(), filter).Decode(anyModuleDb)
	if err != nil {
		return nil, &types.Error{
			Err:     MONGODB_FATAL_ERROR,
			Details: err.Error(),
			Message: "Find one failed",
		}
	}

	return anyModuleDb, nil
}

func (sl *SetterLib) SeedByRandID(randId string) (*types.Entity, *types.Error) {
	
	anyModuleDb, errorFind := sl.FindByRandID(randId)

	if errorFind != nil {
		return nil, errorFind
	}

	sl.redis.Set(anyModuleDb)
	return anyModuleDb, nil
}


// SeedLinked
func (sl *SetterLib) SeedLinked(subtraction int64, latestItemHex string, lastRandId string, anyUUID string) *types.Error {
	if sl.anyModuleCollection == nil {
		return &types.Error{
			Err:     errors.New("anyModuleCollection is nil"),
			Details: "The anyModuleCollection field is not initialized in SetterLib",
			Message: "Unable to delete module",
		}
	}

	var cursor *mongo.Cursor
	var filter bson.D
	var anyModules []types.Entity

	findOptions := options.Find()
	findOptions.SetSort(bson.D{{"_id", -1}})

	if subtraction > 0 {
		lastFetchedObjectId, errorConvertHex := primitive.ObjectIDFromHex(latestItemHex)

		if errorConvertHex != nil {
			return &types.Error{
				Err:     MONGODB_FATAL_ERROR,
				Details: errorConvertHex.Error(),
				Message: "Unable to SeedLinked module",
			}
		}

		numberOfRecordsToFill := DATA_PER_PAGE - subtraction
		findOptions.SetLimit(int64(numberOfRecordsToFill))
		filter = bson.D{{"$and",
			bson.A{
				bson.D{{"anyuuid", anyUUID}},
				bson.D{{"_id", bson.D{{"$lt", lastFetchedObjectId}}}},
			},
		}}
	} else {

		findOptions.SetLimit(int64(DATA_PER_PAGE))
		if lastRandId != "" {

			// findOne validLastUUID first
			anyModule, errorFind := sl.FindByRandID(lastRandId)

			if errorFind != nil {
				return errorFind
			}

			anyModuleID := anyModule.ID
			filter = bson.D{{"$and",
				bson.A{
					bson.D{{"anyuuid", anyUUID}},
					bson.D{{"_id", bson.D{{"$lt", anyModuleID}}}},
				},
			}}

		} else {

			// fetch anyModules from beginning
			filter = bson.D{{"anyuuid", anyUUID}}
		}
	}

	var errorFinds error

	cursor, errorFinds = sl.anyModuleCollection.Find(
		context.TODO(),
		filter,
		findOptions,
	)

	if errorFinds != nil {
		return &types.Error{
			Err:     MONGODB_FATAL_ERROR,
			Details: errorFinds.Error(),
			Message: "Unable to SeedLinked module",
		}
	}

	defer cursor.Close(context.TODO())

	for cursor.Next(context.TODO()) {
		var anyModule types.Entity
		errorDecode := cursor.Decode(&anyModule)

		if errorDecode != nil {
			continue
		}

		sl.redis.Set(&anyModule)
		sl.redis.SetSortedSet(&anyModule)
		anyModules = append(anyModules, anyModule)
	}

	if len(anyModules)  == 0 {
		sl.redis.SetSettled(anyUUID)
	}

	return nil
}

// SeedAll
//  1. Find many from db => data
//
// 2. Loop all data ingest each item & add to sorted set
func (sl *SetterLib) SeedAll(anyUUID string) *types.Error {
	if sl.anyModuleCollection == nil {
		return &types.Error{
			Err:     errors.New("anyModuleCollection is nil"),
			Details: "The anyModuleCollection field is not initialized in SetterLib",
			Message: "Unable to delete module",
		}
	}

	filter := bson.M{"anyuuid": anyUUID}

	//	1. Find many by uuid => data
	cursor, errorFind := sl.anyModuleCollection.Find(
		context.TODO(),
		filter,
	)

	if errorFind != nil {
		return &types.Error{
			Err:     errorFind,
			Details: "Failed to execute find on MongoDB",
			Message: "Find many failed",
		}
	}

	defer cursor.Close(context.TODO())

	// 2. Loop all data ingest each item & add to sorted set
	for cursor.Next(context.TODO()) {
		var anyModule *types.Entity
		errorDecode := cursor.Decode(anyModule)

		if errorDecode != nil {
			continue
		}

		sl.redis.Set(anyModule)
		sl.redis.SetSortedSet(anyModule)
	}

	sl.redis.SetSettled(anyUUID)

	return nil
}

type GetterLib struct {
	redisClient *redis.UniversalClient
	redis       CommonRedis
}

func NewGetterLib(
	redisClient *redis.UniversalClient,
) *GetterLib {
	return &GetterLib{
		redisClient: redisClient,
		redis: CommonRedis{
			client: redisClient,
		},
	}
}

// GetByRandID
// Get cache by randid
func (gl *GetterLib) Get(randid string) (*types.Entity, *types.Error) {
	var anyModule types.Entity
	err := (*gl.redisClient).Get(context.TODO(), randid).Scan(&anyModule)
	if err != nil {
		return nil, &types.Error{
			Err:     err,
			Details: "Failed to retrieve anyModule by RandID from Redis",
			Message: "GetByRandID failed",
		}
	}

	return &anyModule, nil
}

// GetLinked
// Zrevrange base on provided lastRandIds
func (gl *GetterLib) GetLinked(anyUUID string, lastRandIds []string) ([]types.Entity, string, int64, *types.Error) {

	sortedSetKey := "sortedset:" + anyUUID
	var anyModules []types.Entity
	var validLastRandId string
	start := int64(0)
	stop := int64(DATA_PER_PAGE)

	for i := len(lastRandIds) - 1; i >= 0; i-- {
		anyModule, err := gl.Get(lastRandIds[i])

		if err != nil{
			continue
		}

		rank := (*gl.redisClient).ZRevRank(context.TODO(), sortedSetKey, anyModule.RandID)

		if rank.Err() == nil {
			validLastRandId = anyModule.RandID
			start = rank.Val() + 1
			stop = start + DATA_PER_PAGE - 1
			break
		}
	}

	totalItem := gl.redis.TotalItemOnSortedSet(anyUUID)

	if totalItem == 0 {
		return anyModules, validLastRandId, 0, nil
	}

	listRandIds := (*gl.redisClient).ZRevRange(
		context.TODO(),
		sortedSetKey,
		start,
		stop,
	)

	if listRandIds.Err() != nil {
		return nil, "", 0, &types.Error{
			Err:     REDIS_FATAL_ERROR,
			Details: listRandIds.Err().Error(),
			Message: "GetAll operation failed",
		}
	}

	(*gl.redisClient).Expire(
		context.TODO(),
		sortedSetKey,
		SORTED_SET_TTL,
	)

	for i := 0; i < len(listRandIds.Val()); i++ {
		uuid := listRandIds.Val()[i]
		anyModule, errGet := gl.redis.Get(uuid)

		if errGet != nil {
			continue
		}

		anyModules = append(anyModules, *anyModule)
	}

	return anyModules, validLastRandId, start, nil
}

// GetAll
func (gl *GetterLib) GetAll(anyUUID string) ([]types.Entity, *types.Error) {
	var anyModules []types.Entity

	totalItem := gl.redis.TotalItemOnSortedSet(anyUUID)
	sortedSetKey := "sortedset:" + anyUUID

	if totalItem == 0 {
		return anyModules, nil
	}

	listRandIds := (*gl.redisClient).ZRevRange(
		context.TODO(),
		sortedSetKey,
		0,
		-1,
	)

	if listRandIds.Err() != nil {
		return nil, &types.Error{
			Err:     REDIS_FATAL_ERROR,
			Details: listRandIds.Err().Error(),
			Message: "GetAll operation failed",
		}
	}

	(*gl.redisClient).Expire(
		context.TODO(),
		sortedSetKey,
		SORTED_SET_TTL,
	)

	for i := 0; i < len(listRandIds.Val()); i++ {
		uuid := listRandIds.Val()[i]
		anyModule, errGet := gl.redis.Get(uuid)

		if errGet != nil {
			continue
		}

		anyModules = append(anyModules, *anyModule)
	}

	return anyModules, nil
}

type CommonRedis struct {
	client *redis.UniversalClient
}

// Get data
func (cr CommonRedis) Get(key string) (*types.Entity, *types.Error) {
	var anyModule types.Entity
	err := (*cr.client).Get(context.TODO(), key).Scan(&anyModule)
	if err != nil {
		return nil, &types.Error{
			Err:     err,
			Details: "Failed to get value from Redis",
			Message: "Get operation failed",
		}
	}
	return &anyModule, nil
}

// Set data
func (cr CommonRedis) Set(anyModule *types.Entity) *types.Error {

	anyModuleJsonString, errorMarshall := json.Marshal(anyModule)
	if errorMarshall != nil {
		return &types.Error{
			Err:     errorMarshall,
			Details: "Failed to marshal anyModule to JSON",
			Message: "Set operation failed",
		}
	}

	key := "anyModule:" + anyModule.RandID
	err := (*cr.client).Set(context.TODO(), key, anyModuleJsonString, 0).Err()
	if err != nil {
		return &types.Error{
			Err:     err,
			Details: "Failed to set key-value pair in Redis",
			Message: "Set operation failed",
		}
	}
	return nil
}

// Del data
func (cr CommonRedis) Del(anyModule *types.Entity) *types.Error {
	key := "anyModule:" + anyModule.RandID
	err := (*cr.client).Del(context.TODO(), key).Err()
	if err != nil {
		return &types.Error{
			Err:     err,
			Details: "Failed to delete key from Redis",
			Message: "Del operation failed",
		}
	}

	return nil
}


// GetSettled
func (cr CommonRedis) GetSettled(anyUUID string) (bool, *types.Error) {
	key := "sortedset:" + anyUUID + ":settled"
	getSortedSet := (*cr.client).Get(
		context.TODO(),
		key,
	)

	if getSortedSet.Err() != nil {
		return false, &types.Error{
			Err:     REDIS_FATAL_ERROR,
			Details: getSortedSet.Err().Error(),
			Message: "GetSortedSet settled operation failed",
		}
	}

	if getSortedSet.Val() == "1" {
		return true, nil
	}

	return false, nil
}

// SetSettled
func (cr CommonRedis) SetSettled(anyUUID string) *types.Error {
	key := "sortedset:" + anyUUID + ":settled"
	removeSortedSet := (*cr.client).Set(
		context.TODO(),
		key,
		1,
		SORTED_SET_TTL,
	)

	if removeSortedSet.Err() != nil {
		return &types.Error{
			Err:     REDIS_FATAL_ERROR,
			Details: removeSortedSet.Err().Error(),
			Message: "DeleteSortedSet settled operation failed",
		}
	}
	return nil
}

// DelSettled
func (cr CommonRedis) DelSettled(anyUUID string) *types.Error {
	key := "sortedset:" + anyUUID + ":settled"
	removeSortedSet := (*cr.client).Del(
		context.TODO(),
		key,
	)

	if removeSortedSet.Err() != nil {
		return &types.Error{
			Err:     REDIS_FATAL_ERROR,
			Details: removeSortedSet.Err().Error(),
			Message: "DeleteSortedSet settled operation failed",
		}
	}
	return nil
}

// SetSortedSet
// 1. Delete settled key
// 2. Add to sorted set
func (cr CommonRedis) SetSortedSet(anyModule *types.Entity) *types.Error {
	cr.DelSettled(anyModule.AnyUUID)
	key := "sortedset:" + anyModule.AnyUUID

	sortedSetMember := redis.Z{
		Score:  float64(anyModule.CreatedAt),
		Member: anyModule.RandID,
	}

	setSortedSet := (*cr.client).ZAdd(
		context.TODO(),
		key,
		sortedSetMember,
	)

	if setSortedSet.Err() != nil {
		return &types.Error{
			Err:     REDIS_FATAL_ERROR,
			Details: setSortedSet.Err().Error(),
			Message: "SetSortedSet operation failed",
		}
	}

	setExpire := (*cr.client).Expire(
		context.TODO(),
		key,
		SORTED_SET_TTL,
	)

	if !setExpire.Val() {
		return &types.Error{
			Err:     REDIS_FATAL_ERROR,
			Details: setExpire.Err().Error(),
			Message: "SetSortedSet operation failed",
		}
	}

	return nil

}

// DeleteFromSortedSet
func (cr CommonRedis) DeleteFromSortedSet(anyModule *types.Entity) *types.Error {
	key := "sortedset:" + anyModule.AnyUUID
	removeFromSortedSet := (*cr.client).ZRem(
		context.TODO(),
		key,
		anyModule.RandID,
	)

	if removeFromSortedSet.Err() != nil {

		return &types.Error{
			Err:     REDIS_FATAL_ERROR,
			Details: removeFromSortedSet.Err().Error(),
			Message: "DeleteFromSortedSet operation failed",
		}
	}

	return nil
}

// TotalItemOnSortedSet
func (cr CommonRedis) TotalItemOnSortedSet(anyUUID string) int64 {
	key := "sortedset:" + anyUUID
	zCard := (*cr.client).ZCard(context.TODO(), key)

	if zCard.Err() != nil {
		return 0
	}

	return zCard.Val()

}

// DeleteSortedSet
func (cr CommonRedis) DeleteSortedSet(anyUUID string) *types.Error {

	key := "sortedset:" + anyUUID
	removeSortedSet := (*cr.client).Del(
		context.TODO(),
		key,
	)

	if removeSortedSet.Err() != nil {
		return &types.Error{
			Err:     REDIS_FATAL_ERROR,
			Details: removeSortedSet.Err().Error(),
			Message: "DeleteSortedSet operation failed",
		}
	}
	return nil
}
