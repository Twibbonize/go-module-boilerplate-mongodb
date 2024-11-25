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
	mongoCollection *mongo.Collection
	redisClient *redis.UniversalClient
	redis           CommonRedis
}

func NewSetterLib(
	mongoCollection *mongo.Collection,
	redis *redis.UniversalClient,
) *SetterLib {
	return &SetterLib{
		mongoCollection: mongoCollection,
		redisClient: redis,
		redis: CommonRedis{
			client: redis,
		},
	}
}

// Create 
//	- Insert db
//	- Set jsonstr to cache 
//	- Add to sorted set
func (sl *SetterLib) Create(entity *types.Entity) *types.Error {
	if sl.mongoCollection == nil {
		return &types.Error{
			Err:     errors.New("mongoCollection is nil"),
			Details: "The mongoCollection field is not initialized in SetterLib",
			Message: "can't work with 42",
		}
	}

	err := sl.redis.Set(entity)
	if err != nil {
		return err
	}

	err = sl.redis.SetSortedSet(entity)
	if err != nil {
		return err
	}

	return nil
}

// Update 
//	- Update one db
//	- Set to cache 
func (sl *SetterLib) Update(entity *types.Entity) *types.Error {
	if sl.mongoCollection == nil {
		return &types.Error{
			Err:     errors.New("mongoCollection is nil"),
			Details: "The mongoCollection field is not initialized in SetterLib",
			Message: "Unable to update entity",
		}
	}

	filter := bson.M{"_id": entity.ID}
	update := bson.M{"$set": bson.M{
		"updatedat": time.Now().UnixMilli(),
		"randId":    entity.RandID,
	}}

	_, err := sl.mongoCollection.UpdateOne(context.TODO(), filter, update)
	if err != nil {
		return &types.Error{
			Err:     err,
			Details: "Failed to execute update on MongoDB",
			Message: "Update failed",
		}
	}

	setErr := sl.redis.Set(entity)
	if setErr != nil {
		return setErr
	}

	return nil
}

// Delete
//	- Del jsonstr from cache 
//	- Remove item from sorted set
func (sl *SetterLib) Delete(entity *types.Entity) *types.Error {
	if sl.mongoCollection == nil {
		return &types.Error{
			Err:     errors.New("mongoCollection is nil"),
			Details: "The mongoCollection field is not initialized in SetterLib",
			Message: "Unable to delete entity",
		}
	}

	filter := bson.M{"_id": entity.ID}
	_, err := sl.mongoCollection.DeleteOne(context.TODO(), filter)
	if err != nil {
		return &types.Error{
			Err:     err,
			Details: "Failed to execute delete on MongoDB",
			Message: "Delete failed",
		}
	}

	sl.redis.Del(entity)
	sl.redis.DeleteFromSortedSet(entity)

	return nil
}

// TODO
// DeleteManyByAnyUUID
//	- Delete many by uuid from db
//	- Loop to delete all cache key 
//	- Delete sorted set key
func (sl *SetterLib) DeleteManyByAnyUUID(anyUUID string) *types.Error {
	if sl.mongoCollection == nil {
		return &types.Error{
			Err:     errors.New("mongoCollection is nil"),
			Details: "The mongoCollection field is not initialized in SetterLib",
			Message: "Unable to delete entities",
		}
	}

	filter := bson.M{"anyuuid": anyUUID}
	_, err := sl.mongoCollection.DeleteMany(context.TODO(), filter)
	if err != nil {
		return &types.Error{
			Err:     err,
			Details: "Failed to execute delete on MongoDB",
			Message: "Delete many failed",
		}
	}

	return nil
}


// TODO
// FindByUUID: secured
//	- Find one by uuid from db
//	- Set to cache
//	- Set randid translation
func (sl *SetterLib) FindByUUID(uuid string) (*types.Entity, *types.Error) {
	return nil, nil
}

// TODO
// FindByRandID
//	- Find one by randid from db
//	- Set to cache
//	- Set randid translation
func (sl *SetterLib) FindByRandID(randid string) (*types.Entity, *types.Error) {
	return nil, nil
}


// TODO
// SeedLinked
func (sl *SetterLib) SeedLinked(subtraction int64, latestItemHex string, lastUUID string, anyUUID string) *types.Error {
	return nil
}

// TODO
// SeedAll
func (sl *SetterLib) SeedAll(anyUUID string) *types.Error {
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


// TODO
// GetByUUID: secured
func (gl *GetterLib) GetByUUID(uuid string) (*types.Entity, *types.Error) {
	var entity types.Entity
	err := (*gl.redisClient).Get(context.TODO(), uuid).Scan(&entity)
	if err != nil {
		return nil, &types.Error{
			Err:     err,
			Details: "Failed to retrieve entity by UUID from Redis",
			Message: "GetByUUID failed",
		}
	}

	return &entity, nil
}


// TODO
// GetByRandID
func (gl *GetterLib) GetByRandID(randid string) (*types.Entity, *types.Error) {
	var entity types.Entity
	err := (*gl.redisClient).Get(context.TODO(), randid).Scan(&entity)
	if err != nil {
		return nil, &types.Error{
			Err:     err,
			Details: "Failed to retrieve entity by RandID from Redis",
			Message: "GetByRandID failed",
		}
	}

	return &entity, nil
}


// TODO
// GetLinked
func (gl *GetterLib) GetLinked(anyUUID string, lastRandIds []string) ([]types.Entity, string, int64, *types.Error) {
	// Example implementation to fetch linked items
	var entities []types.Entity
	for _, randId := range lastRandIds {
		entity, err := gl.GetByRandID(randId)
		if err != nil {
			continue // Skip any failed retrievals
		}
		entities = append(entities, *entity)
	}

	nextCursor := "" // logic to compute next cursor
	totalCount := int64(len(entities))

	return entities, nextCursor, totalCount, nil
}


// TODO
// GetAll
func GetAll(anyUUID string) ([]types.Entity, *types.Error) {
	return nil, nil
}

type CommonRedis struct {
	client *redis.UniversalClient
}

func (cr CommonRedis) Get(key string) (*types.Entity, *types.Error) {
	var entity types.Entity
	err := (*cr.client).Get(context.TODO(), key).Scan(&entity)
	if err != nil {
		return nil, &types.Error{
			Err:     err,
			Details: "Failed to get value from Redis",
			Message: "Get operation failed",
		}
	}
	return &entity, nil
}

// TODO
// Set
func (cr CommonRedis) Set(entity *types.Entity) *types.Error {

	entityJsonString, errorMarshall := json.Marshal(entity)

	if errorMarshall != nil {
		return &types.Error{
			Err:     errorMarshall,
			Details: "Failed to marshal entity to JSON",
			Message: "Set operation failed",
		}
	}

	key := "submission:" + entity.UUID

	err := (*cr.client).Set(context.TODO(), key, entityJsonString, 0).Err()
	if err != nil {
		return &types.Error{
			Err:     err,
			Details: "Failed to set key-value pair in Redis",
			Message: "Set operation failed",
		}
	}
	return nil
}


// TODO
// Del
func (cr CommonRedis) Del(entity *types.Entity) *types.Error {
	key := "submission:" + entity.UUID

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

// TODO
// SetRandID
func (cr CommonRedis) SetRandID(types * types.Entity) *types.Error {
	return nil
}

// TODO
// DelRandId
func (cr CommonRedis) DelRandId(types * types.Entity) *types.Error {
	return nil
}


// TODO
// GetSettled
func (cr CommonRedis) GetSettled(anyUUID string) (bool, *types.Error) {
	return false, nil
}


// TODO
// SetSettled
func (cr CommonRedis) SetSettled(anyUUID string) *types.Error {
	return nil
}


// TODO
// DelSettled
func (cr CommonRedis) DelSettled(anyUUID string) *types.Error {
	return nil
}

// TODO
// SetSortedSet
func (cr CommonRedis) SetSortedSet(types * types.Entity) *types.Error {
	return nil
}

// TODO
// DeleteFromSortedSet
func (cr CommonRedis) DeleteFromSortedSet(types * types.Entity) *types.Error {
	return nil
}

// TODO
// TotalItemOnSortedSet
func (cr CommonRedis) TotalItemOnSortedSet(anyUUID string) (int64, *types.Error) {
	return 0, nil
}

// TODO
// DeleteSortedSet
func (cr CommonRedis) DeleteSortedSet(types * types.Entity) *types.Error {
	return nil
}
