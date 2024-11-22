package moduleboilerplate

import (
	"math/rand"
	"time"

	"github.com/Twibbonize/go-module-boilerplate-mongodb/types"
	"github.com/google/uuid"
	"github.com/redis/go-redis/v9"
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
	redis           CommonRedis
}

func NewSetterLib(
	mongoCollection *mongo.Collection,
	redis *redis.UniversalClient,
) *SetterLib {
	return &SetterLib{
		mongoCollection: mongoCollection,
		redis: CommonRedis{
			client: redis,
		},
	}
}

func (sl *SetterLib) Create(*types.Entity) *types.Error {
	return nil
}

func (sl *SetterLib) Update(*types.Entity) *types.Error {
	return nil
}

func (sl *SetterLib) Delete(*types.Entity) *types.Error {
	return nil
}

func (sl *SetterLib) DeleteManyByAnyUUID(anyUUID string) *types.Error {
	return nil
}

// secured
func (sl *SetterLib) FindByUUID(uuid string) (*types.Entity, *types.Error) {
	return nil, nil
}

func (sl *SetterLib) FindByRandID(randid string) (*types.Entity, *types.Error) {
	return nil, nil
}

// secured
func (sl *SetterLib) SeedByUUID(uuid string) *types.Error {
	return nil
}

func (sl *SetterLib) SeedByRandID(randid string) *types.Error {
	return nil
}

func (sl *SetterLib) SeedLinked(subtraction int64, latestItemHex string, lastUUID string, anyUUID string) *types.Error {
	return nil
}

func (sl *SetterLib) SeedAll(anyUUID string) *types.Error {
	return nil
}

type GetterLib struct {
	redisClient redis.UniversalClient
	redis       CommonRedis
}

func NewGetterLib(
	redisClient redis.UniversalClient,
) *GetterLib {
	return &GetterLib{
		redisClient: redisClient,
		redis: CommonRedis{
			client: &redisClient,
		},
	}
}

// secured
func (gl *GetterLib) GetByUUID(uuid string) (*types.Entity, *types.Error) {
	return nil, nil
}
func (gl *GetterLib) GetByRandID(randid string) (*types.Entity, *types.Error) {
	return nil, nil
}
func (gl *GetterLib) GetLinked(anyUUID string, lastRandIds []string) ([]types.Entity, string, int64, *types.Error) {
	return nil, "", 0, nil
}

func GetAll(anyUUID string) ([]types.Entity, *types.Error) {
	return nil, nil
}

type CommonRedis struct {
	client *redis.UniversalClient
}

func (cr *CommonRedis) Get() (*types.Entity, *types.Error) {
	return nil, nil
}

func (cr *CommonRedis) Set() *types.Error {
	return nil
}

func (cr *CommonRedis) Del() *types.Error {
	return nil
}

func (cr *CommonRedis) SetRandID() *types.Error {
	return nil
}

func (cr *CommonRedis) DelRandId() *types.Error {
	return nil
}

func (cr *CommonRedis) GetSettled() (bool, *types.Error) {
	return false, nil
}

func (cr *CommonRedis) SetSettled() *types.Error {
	return nil
}

func (cr *CommonRedis) DelSettled() *types.Error {
	return nil
}

func (cr *CommonRedis) SetSortedSet() *types.Error {
	return nil
}

func (cr *CommonRedis) DeleteFromSortedSet() *types.Error {
	return nil
}

func (cr *CommonRedis) TotalItemOnSortedSet() (int64, *types.Error) {
	return 0, nil
}

func (cr *CommonRedis) DeleteSortedSet() *types.Error {
	return nil
}
