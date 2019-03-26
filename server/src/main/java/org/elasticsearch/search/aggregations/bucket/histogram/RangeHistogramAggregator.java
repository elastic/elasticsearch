package org.elasticsearch.search.aggregations.bucket.histogram;

import org.apache.lucene.index.LeafReaderContext;
import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.util.LongHash;
import org.elasticsearch.search.DocValueFormat;
import org.elasticsearch.search.aggregations.Aggregator;
import org.elasticsearch.search.aggregations.AggregatorFactories;
import org.elasticsearch.search.aggregations.BucketOrder;
import org.elasticsearch.search.aggregations.InternalAggregation;
import org.elasticsearch.search.aggregations.InternalOrder;
import org.elasticsearch.search.aggregations.LeafBucketCollector;
import org.elasticsearch.search.aggregations.bucket.BucketsAggregator;
import org.elasticsearch.search.aggregations.pipeline.PipelineAggregator;
import org.elasticsearch.search.aggregations.support.ValuesSource;
import org.elasticsearch.search.internal.SearchContext;

import java.io.IOException;
import java.util.List;
import java.util.Map;

public class RangeHistogramAggregator extends BucketsAggregator {
    private final ValuesSource.Bytes valuesSource;
    private final DocValueFormat formatter;
    private final double interval, offset;
    private final BucketOrder order;
    private final boolean keyed;
    private final long minDocCount;
    private final double minBound, maxBound;

    private final LongHash bucketOrds;

    RangeHistogramAggregator(String name, AggregatorFactories factories, double interval, double offset,
                               BucketOrder order, boolean keyed, long minDocCount, double minBound, double maxBound,
                               @Nullable ValuesSource.Bytes valuesSource, DocValueFormat formatter,
                               SearchContext context, Aggregator parent,
                               List<PipelineAggregator> pipelineAggregators, Map<String, Object> metaData) throws IOException {

        super(name, factories, context, parent, pipelineAggregators, metaData);
        if (interval <= 0) {
            throw new IllegalArgumentException("interval must be positive, got: " + interval);
        }
        this.interval = interval;
        this.offset = offset;
        this.order = InternalOrder.validate(order, this);
        this.keyed = keyed;
        this.minDocCount = minDocCount;
        this.minBound = minBound;
        this.maxBound = maxBound;
        this.valuesSource = valuesSource;
        this.formatter = formatter;

        bucketOrds = new LongHash(1, context.bigArrays());
    }

    @Override
    protected LeafBucketCollector getLeafCollector(LeafReaderContext ctx, LeafBucketCollector sub) throws IOException {
        throw new UnsupportedOperationException();
    }

    @Override
    public InternalAggregation buildAggregation(long bucket) throws IOException {
        throw new UnsupportedOperationException();
    }

    @Override
    public InternalAggregation buildEmptyAggregation() {
        throw new UnsupportedOperationException();
    }
}
