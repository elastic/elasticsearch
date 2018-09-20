package org.elasticsearch.ingest;

import org.elasticsearch.common.metrics.CounterMetric;
import org.elasticsearch.common.metrics.MeanMetric;

class IngestMetric {

    private final MeanMetric ingestMetric = new MeanMetric();
    private final CounterMetric ingestCurrent = new CounterMetric();
    private final CounterMetric ingestCount = new CounterMetric();
    private final CounterMetric ingestFailed = new CounterMetric();

    void preIngest() {
        ingestCurrent.inc();
    }

    void postIngest(long ingestTimeInMillis) {
        ingestCurrent.dec();
        ingestMetric.inc(ingestTimeInMillis);
        ingestCount.inc();
    }

    void ingestFailed() {
        ingestFailed.inc();
    }

    void add(IngestMetric metrics){
        ingestCount.inc(metrics.ingestCount.count());
        ingestMetric.inc(metrics.ingestMetric.sum());
        ingestFailed.inc(metrics.ingestFailed.count());
    }

    IngestStats.Stats createStats() {
        return new IngestStats.Stats(ingestCount.count(), ingestMetric.sum(), ingestCurrent.count(), ingestFailed.count());
    }
}
