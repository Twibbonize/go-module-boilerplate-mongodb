package types

import "go.mongodb.org/mongo-driver/bson/primitive"

type Entity struct {
	ID        primitive.ObjectID `json:"-" bson:"_id"`
	UUID      string             `json:"uuid" bson:"uuid"`
	RandID    string             `json:"randId" bson:"randid"`
	CreatedAt int64              `json:"createdAt" bson:"createdat"`
	UpdatedAt int64              `json:"updatedAt" bson:"updatedat"`
	AnyUUID   string             `json:"anyuuid" bson:"anyuuid"`
}

type Error struct {
	Err     error
	Details string
	Message string
}
