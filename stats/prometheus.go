package stats

import (
	"context"
	"net"
	"net/http"
	"strconv"
	"time"

	"github.com/9seconds/mtg/v2/events"
	"github.com/9seconds/mtg/v2/mtglib"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

type prometheusProcessor struct {
	streams map[string]*streamInfo
	factory *PrometheusFactory
}

func (p prometheusProcessor) EventStart(evt mtglib.EventStart) {
	info := acquireStreamInfo()
	info.startedAt = time.Now()

	if evt.RemoteIP.To4() != nil {
		info.tags[TagIPFamily] = TagIPFamilyIPv4
	} else {
		info.tags[TagIPFamily] = TagIPFamilyIPv6
	}

	p.streams[evt.StreamID()] = info

	p.factory.metricClientConnections.
		WithLabelValues(info.tags[TagIPFamily]).
		Inc()
}

func (p prometheusProcessor) EventConnectedToDC(evt mtglib.EventConnectedToDC) {
	info, ok := p.streams[evt.StreamID()]
	if !ok {
		return
	}

	info.tags[TagTelegramIP] = evt.RemoteIP.String()
	info.tags[TagDC] = strconv.Itoa(evt.DC)

	p.factory.metricTelegramConnections.
		WithLabelValues(info.tags[TagTelegramIP], info.tags[TagDC]).
		Inc()
}

func (p prometheusProcessor) EventDomainFronting(evt mtglib.EventDomainFronting) {
	info, ok := p.streams[evt.StreamID()]
	if !ok {
		return
	}

	info.isDomainFronted = true

	p.factory.metricDomainFronting.Inc()
	p.factory.metricDomainFrontingByIP.
		WithLabelValues(evt.RemoteIP.String()).
		Inc()
	p.factory.metricDomainFrontingConnections.
		WithLabelValues(info.tags[TagIPFamily]).
		Inc()
}

func (p prometheusProcessor) EventTraffic(evt mtglib.EventTraffic) {
	info, ok := p.streams[evt.StreamID()]
	if !ok {
		return
	}

	direction := getDirection(evt.IsRead)

	if info.isDomainFronted {
		p.factory.metricDomainFrontingTraffic.
			WithLabelValues(direction).
			Add(float64(evt.Traffic))
	} else {
		p.factory.metricTelegramTraffic.
			WithLabelValues(info.tags[TagTelegramIP], info.tags[TagDC], direction).
			Add(float64(evt.Traffic))
	}
}

func (p prometheusProcessor) EventFinish(evt mtglib.EventFinish) {
	info, ok := p.streams[evt.StreamID()]
	if !ok {
		return
	}

	defer func() {
		delete(p.streams, evt.StreamID())
		releaseStreamInfo(info)
	}()

	if !info.startedAt.IsZero() {
		p.factory.metricConnectionDuration.
			Observe(time.Since(info.startedAt).Seconds())
	}

	p.factory.metricClientConnections.
		WithLabelValues(info.tags[TagIPFamily]).
		Dec()

	if info.isDomainFronted {
		p.factory.metricDomainFrontingConnections.
			WithLabelValues(info.tags[TagIPFamily]).
			Dec()
	} else if telegramIP, ok := info.tags[TagTelegramIP]; ok {
		p.factory.metricTelegramConnections.
			WithLabelValues(telegramIP, info.tags[TagDC]).
			Dec()
	}
}

func (p prometheusProcessor) EventConcurrencyLimited(_ mtglib.EventConcurrencyLimited) {
	p.factory.metricConcurrencyLimited.Inc()
}

func (p prometheusProcessor) EventIPBlocklisted(evt mtglib.EventIPBlocklisted) {
	tag := TagIPListBlock
	if !evt.IsBlockList {
		tag = TagIPListAllow
	}

	p.factory.metricIPBlocklisted.WithLabelValues(tag).Inc()
	p.factory.metricIPBlocklistedByIP.WithLabelValues(evt.RemoteIP.String(), tag).Inc()
}

func (p prometheusProcessor) EventReplayAttack(_ mtglib.EventReplayAttack) {
	p.factory.metricReplayAttacks.Inc()
}

func (p prometheusProcessor) EventIPListSize(evt mtglib.EventIPListSize) {
	tag := TagIPListBlock
	if !evt.IsBlockList {
		tag = TagIPListAllow
	}

	p.factory.metricIPListSize.WithLabelValues(tag).Set(float64(evt.Size))
}

func (p prometheusProcessor) Shutdown() {
	for k, v := range p.streams {
		releaseStreamInfo(v)
		delete(p.streams, k)
	}
}

// PrometheusFactory is a factory of [events.Observer] which collect
// information in a format suitable for Prometheus.
//
// This factory can also serve on a given listener. In that case it starts HTTP
// server with a single endpoint - a Prometheus-compatible scrape output.
type PrometheusFactory struct {
	httpServer *http.Server

	metricClientConnections         *prometheus.GaugeVec
	metricTelegramConnections       *prometheus.GaugeVec
	metricDomainFrontingConnections *prometheus.GaugeVec
	metricIPListSize                *prometheus.GaugeVec

	metricTelegramTraffic       *prometheus.CounterVec
	metricDomainFrontingTraffic *prometheus.CounterVec
	metricIPBlocklisted         *prometheus.CounterVec

	metricDomainFronting       prometheus.Counter
	metricDomainFrontingByIP   *prometheus.CounterVec
	metricIPBlocklistedByIP    *prometheus.CounterVec
	metricConnectionDuration   prometheus.Histogram
	metricConcurrencyLimited   prometheus.Counter
	metricReplayAttacks        prometheus.Counter
}

// Make builds a new observer.
func (p *PrometheusFactory) Make() events.Observer {
	return prometheusProcessor{
		streams: make(map[string]*streamInfo),
		factory: p,
	}
}

// Serve starts an HTTP server on a given listener.
func (p *PrometheusFactory) Serve(listener net.Listener) error {
	return p.httpServer.Serve(listener) //nolint: wrapcheck
}

// Close stops a factory. Please pay attention that underlying listener
// is not closed.
func (p *PrometheusFactory) Close() error {
	return p.httpServer.Shutdown(context.Background()) //nolint: wrapcheck
}

// NewPrometheus builds an events.ObserverFactory which can serve HTTP
// endpoint with Prometheus scrape data.
func NewPrometheus(metricPrefix, httpPath string) *PrometheusFactory { //nolint: funlen
	registry := prometheus.NewPedanticRegistry()
	httpHandler := promhttp.HandlerFor(registry, promhttp.HandlerOpts{
		EnableOpenMetrics: true,
	})
	mux := http.NewServeMux()

	mux.Handle(httpPath, httpHandler)

	factory := &PrometheusFactory{
		httpServer: &http.Server{
			Handler: mux,
		},

		metricClientConnections: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Namespace: metricPrefix,
			Name:      MetricClientConnections,
			Help:      "A number of actively processing client connections.",
		}, []string{TagIPFamily}),
		metricTelegramConnections: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Namespace: metricPrefix,
			Name:      MetricTelegramConnections,
			Help:      "A number of connections to Telegram servers.",
		}, []string{TagTelegramIP, TagDC}),
		metricDomainFrontingConnections: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Namespace: metricPrefix,
			Name:      MetricDomainFrontingConnections,
			Help:      "A number of connections which talk to front domain.",
		}, []string{TagIPFamily}),
		metricIPListSize: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Namespace: metricPrefix,
			Name:      MetricIPListSize,
			Help:      "A size of the ip list (blocklist or allowlist)",
		}, []string{TagIPList}),

		metricTelegramTraffic: prometheus.NewCounterVec(prometheus.CounterOpts{
			Namespace: metricPrefix,
			Name:      MetricTelegramTraffic,
			Help:      "Traffic which is generated talking with Telegram servers.",
		}, []string{TagTelegramIP, TagDC, TagDirection}),
		metricDomainFrontingTraffic: prometheus.NewCounterVec(prometheus.CounterOpts{
			Namespace: metricPrefix,
			Name:      MetricDomainFrontingTraffic,
			Help:      "Traffic which is generated talking with front domain.",
		}, []string{TagDirection}),
		metricIPBlocklisted: prometheus.NewCounterVec(prometheus.CounterOpts{
			Namespace: metricPrefix,
			Name:      MetricIPBlocklisted,
			Help:      "A number of rejected sessions due to ip blocklisting.",
		}, []string{TagIPList}),

		metricDomainFronting: prometheus.NewCounter(prometheus.CounterOpts{
			Namespace: metricPrefix,
			Name:      MetricDomainFronting,
			Help:      "A number of routings to front domain.",
		}),
		metricDomainFrontingByIP: prometheus.NewCounterVec(prometheus.CounterOpts{
			Namespace: metricPrefix,
			Name:      MetricDomainFrontingByIP,
			Help:      "A number of domain fronting events grouped by client IP.",
		}, []string{TagClientIP}),
		metricIPBlocklistedByIP: prometheus.NewCounterVec(prometheus.CounterOpts{
			Namespace: metricPrefix,
			Name:      MetricIPBlocklistedByIP,
			Help:      "A number of IP blocklist events grouped by client IP.",
		}, []string{TagClientIP, TagIPList}),
		metricConnectionDuration: prometheus.NewHistogram(prometheus.HistogramOpts{
			Namespace: metricPrefix,
			Name:      MetricConnectionDuration,
			Help:      "Duration of client connections in seconds.",
			Buckets:   []float64{1, 5, 15, 30, 60, 120, 300, 600, 1800, 3600},
		}),
		metricConcurrencyLimited: prometheus.NewCounter(prometheus.CounterOpts{
			Namespace: metricPrefix,
			Name:      MetricConcurrencyLimited,
			Help:      "A number of sessions that were rejected by concurrency limiter.",
		}),
		metricReplayAttacks: prometheus.NewCounter(prometheus.CounterOpts{
			Namespace: metricPrefix,
			Name:      MetricReplayAttacks,
			Help:      "A number of detected replay attacks.",
		}),
	}

	registry.MustRegister(factory.metricClientConnections)
	registry.MustRegister(factory.metricTelegramConnections)
	registry.MustRegister(factory.metricDomainFrontingConnections)
	registry.MustRegister(factory.metricIPListSize)

	registry.MustRegister(factory.metricTelegramTraffic)
	registry.MustRegister(factory.metricDomainFrontingTraffic)
	registry.MustRegister(factory.metricIPBlocklisted)

	registry.MustRegister(factory.metricDomainFronting)
	registry.MustRegister(factory.metricDomainFrontingByIP)
	registry.MustRegister(factory.metricIPBlocklistedByIP)
	registry.MustRegister(factory.metricConnectionDuration)
	registry.MustRegister(factory.metricConcurrencyLimited)
	registry.MustRegister(factory.metricReplayAttacks)

	return factory
}
