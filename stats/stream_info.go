package stats

import (
	"time"

	statsd "github.com/smira/go-statsd"
)

type streamInfo struct {
	isDomainFronted bool
	startedAt       time.Time
	tags            map[string]string
}

func (s streamInfo) T(key string) statsd.Tag {
	return statsd.StringTag(key, s.tags[key])
}

func (s *streamInfo) Reset() {
	s.isDomainFronted = false
	s.startedAt = time.Time{}

	for k := range s.tags {
		delete(s.tags, k)
	}
}

func getDirection(isRead bool) string {
	if isRead { // for telegram
		return TagDirectionToClient
	}

	return TagDirectionFromClient
}
