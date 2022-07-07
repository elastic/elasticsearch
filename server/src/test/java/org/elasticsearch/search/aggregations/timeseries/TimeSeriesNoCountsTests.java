/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.search.aggregations.timeseries;

import org.apache.lucene.index.DocValues;
import org.apache.lucene.index.NumericDocValues;
import org.apache.lucene.index.SortedDocValues;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.search.ScoreMode;
import org.apache.lucene.util.PriorityQueue;
import org.elasticsearch.Version;
import org.elasticsearch.action.admin.indices.forcemerge.ForceMergeRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.mapper.TimeSeriesIdFieldMapper;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.MatchAllQueryBuilder;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.index.query.RangeQueryBuilder;
import org.elasticsearch.index.query.TermsQueryBuilder;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.plugins.SearchPlugin;
import org.elasticsearch.search.aggregations.AbstractAggregationBuilder;
import org.elasticsearch.search.aggregations.AggregationBuilder;
import org.elasticsearch.search.aggregations.AggregationExecutionContext;
import org.elasticsearch.search.aggregations.AggregationReduceContext;
import org.elasticsearch.search.aggregations.Aggregator;
import org.elasticsearch.search.aggregations.AggregatorBase;
import org.elasticsearch.search.aggregations.AggregatorFactories;
import org.elasticsearch.search.aggregations.AggregatorFactory;
import org.elasticsearch.search.aggregations.CardinalityUpperBound;
import org.elasticsearch.search.aggregations.InternalAggregation;
import org.elasticsearch.search.aggregations.LeafBucketCollector;
import org.elasticsearch.search.aggregations.bucket.terms.StringTerms;
import org.elasticsearch.search.aggregations.bucket.terms.TermsAggregationBuilder;
import org.elasticsearch.search.aggregations.support.AggregationContext;
import org.elasticsearch.search.aggregations.support.ValueType;
import org.elasticsearch.search.aggregations.support.ValuesSource;
import org.elasticsearch.search.aggregations.support.ValuesSourceConfig;
import org.elasticsearch.test.ESSingleNodeTestCase;
import org.elasticsearch.xcontent.ObjectParser;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

import static org.elasticsearch.action.support.WriteRequest.RefreshPolicy.IMMEDIATE;

public class TimeSeriesNoCountsTests extends ESSingleNodeTestCase {

    @Override
    protected Collection<Class<? extends Plugin>> getPlugins() {
        return pluginList(TestPlugin.class);
    }

    public void testWithMatchAllQuery() throws Exception {
        testWithQuery(new MatchAllQueryBuilder());
    }

    public void testWithQueryInDimension() throws Exception {
        testWithQuery(new TermsQueryBuilder("asset_id", "asset1", "asset9"));
    }

    public void testWithQueryInMetric() throws Exception {
        testWithQuery(new RangeQueryBuilder("metric1").gt(0L).lt(Long.MAX_VALUE / 2));
    }

    public void testWithQueryInDimensionAndMetric() throws Exception {
        BoolQueryBuilder queryBuilder = QueryBuilders.boolQuery();
        queryBuilder.filter(new TermsQueryBuilder("asset_id", "asset1", "asset9"));
        queryBuilder.filter(new RangeQueryBuilder("metric1").gt(0L).lt(Long.MAX_VALUE / 2));
        testWithQuery(queryBuilder);
    }

    private void testWithQuery(QueryBuilder queryBuilder) throws Exception {
        XContentBuilder xcb = XContentFactory.jsonBuilder()
            .startObject()
            .startObject("properties")
            .startObject("@timestamp")
            .field("type", "date")
            .endObject()
            .startObject("asset_id")
            .field("type", "keyword")
            .field("time_series_dimension", true)
            .endObject()
            .startObject("metric1")
            .field("type", "long")
            .endObject()
            .startObject("metric2")
            .field("type", "long")
            .endObject()
            .endObject()
            .endObject();

        Settings.Builder s = Settings.builder()
            .put("index.mode", "time_series")
            .put("index.routing_path", "asset_id")
            .put("index.number_of_shards", 1);

        client().admin().indices().prepareCreate("test").setMapping(xcb).setSettings(s).get();
        ensureGreen();

        for (int k = 0; k < 10; k++) {
            for (int i = 0; i < 10; i++) {
                for (int j = 0; j < randomIntBetween(1, 20); j++) {
                    xcb = XContentFactory.jsonBuilder()
                        .startObject()
                        .field("asset_id", "asset" + i)
                        .field("@timestamp", (k * 20) + j)
                        .field("metric1", rarely() ? null : randomLong())
                        .field("metric2", rarely() ? null : randomLong())
                        .endObject();
                    client().prepareIndex("test").setSource(xcb).setRefreshPolicy(IMMEDIATE).get();
                }
            }
        }

        if (randomBoolean()) {
            client().admin().indices().forceMerge(new ForceMergeRequest().indices("test").maxNumSegments(1)).get();
        }
        ensureGreen();

        for (int size = 1; size < 10; size++) {
            TimeSeriesAggregationBuilder builder = new TimeSeriesAggregationBuilder("by_time_series", true, false);

            TestAggregationBuilder topMetric1 = new TestAggregationBuilder("metric1_last_value", size);
            TestAggregationBuilder topMetric2 = new TestAggregationBuilder("metric2_last_value", 10 - size);
            builder.subAggregation(topMetric1);
            builder.subAggregation(topMetric2);

            // query as time series
            SearchResponse searchResponse1 = client().prepareSearch("test").setQuery(queryBuilder).addAggregation(builder).setSize(0).get();

            TermsAggregationBuilder terms = new TermsAggregationBuilder("by_asset_id").size(1000).field("asset_id");
            terms.subAggregation(topMetric1);
            terms.subAggregation(topMetric2);

            // query as standard index
            SearchResponse searchResponse2 = client().prepareSearch("test").setQuery(queryBuilder).addAggregation(terms).setSize(0).get();
            InternalTimeSeries ts1 = searchResponse1.getAggregations().get("by_time_series");
            StringTerms ts2 = searchResponse2.getAggregations().get("by_asset_id");
            assertEquals(ts1.getBuckets().size(), ts2.getBuckets().size());
            for (int i = 0; i < ts1.getBuckets().size(); i++) {
                assertBucket(ts2, ts1.getBuckets().get(i));
            }
        }
    }

    private void assertBucket(StringTerms ts2, InternalTimeSeries.InternalBucket bucket) {
        for (StringTerms.Bucket bucket1 : ts2.getBuckets()) {
            if (bucket1.getKeyAsString().equals(bucket.getKey().get("asset_id"))) {
                {
                    TestInternalAggregation itp1 = bucket.getAggregations().get("metric1_last_value");
                    TestInternalAggregation itp2 = bucket1.getAggregations().get("metric1_last_value");
                    assertEquals(itp2, itp1);
                }
                {
                    TestInternalAggregation itp1 = bucket.getAggregations().get("metric2_last_value");
                    TestInternalAggregation itp2 = bucket1.getAggregations().get("metric2_last_value");
                    assertEquals(itp2, itp1);
                }
                return;
            }
        }
        fail("not found");
    }

    public static class TestPlugin extends Plugin implements SearchPlugin {
        @Override
        public List<AggregationSpec> getAggregations() {
            return Collections.singletonList(
                new AggregationSpec(TestAggregationBuilder.NAME, TestAggregationBuilder::new, TestAggregationBuilder.PARSER)
                    .addResultReader(TestInternalAggregation::new)
            );
        }

        @Override
        public List<NamedWriteableRegistry.Entry> getNamedWriteables() {
            return List.of(
                new NamedWriteableRegistry.Entry(TestInternalAggregation.class, TestInternalAggregation.NAME, TestInternalAggregation::new)
            );
        }
    }

    private static class TestAggregationBuilder extends AbstractAggregationBuilder<TestAggregationBuilder> {
        static final String NAME = "test";

        private static final ObjectParser<TestAggregationBuilder, String> PARSER = ObjectParser.fromBuilder(
            NAME,
            TestAggregationBuilder::new
        );

        private final int size;

        TestAggregationBuilder(String name) {
            this(name, 1);
        }

        TestAggregationBuilder(String name, int size) {
            super(name);
            this.size = size;
        }

        TestAggregationBuilder(StreamInput input) throws IOException {
            super(input);
            size = input.readInt();
        }

        @Override
        protected void doWriteTo(StreamOutput out) throws IOException {
            out.writeInt(size);
        }

        @Override
        protected AggregatorFactory doBuild(
            AggregationContext context,
            AggregatorFactory parent,
            AggregatorFactories.Builder subFactoriesBuilder
        ) throws IOException {
            ValuesSourceConfig config = ValuesSourceConfig.resolve(context, ValueType.DATE, "@timestamp", null, null, null, null, null);
            return new AggregatorFactory(name, context, parent, subFactoriesBuilder, metadata) {
                @Override
                protected Aggregator createInternal(Aggregator parent, CardinalityUpperBound cardinality, Map<String, Object> metadata) {
                    return new TestAggregator(name, parent, config, size);
                }
            };
        }

        @Override
        protected XContentBuilder internalXContent(XContentBuilder builder, Params params) throws IOException {
            return builder;
        }

        @Override
        protected AggregationBuilder shallowCopy(AggregatorFactories.Builder factoriesBuilder, Map<String, Object> metadata) {
            return new TestAggregationBuilder(name);
        }

        @Override
        public BucketCardinality bucketCardinality() {
            return BucketCardinality.NONE;
        }

        @Override
        public String getType() {
            return "test";
        }

        @Override
        public long bytesToPreallocate() {
            return 0;
        }

        @Override
        public Version getMinimalSupportedVersion() {
            return Version.V_EMPTY;
        }
    }

    /**
     * A test aggregator that extends {@link Aggregator} instead of {@link AggregatorBase}
     * to avoid tripping the circuit breaker when executing on a shard.
     */
    private static class TestAggregator extends Aggregator {
        private final String name;
        private final Aggregator parent;
        private final int size;
        private final Map<Long, PriorityQueue<Long>> collector = new HashMap<>();
        private final ValuesSource.Numeric valuesSource;

        private TestAggregator(String name, Aggregator parent, ValuesSourceConfig config, int size) {
            this.name = name;
            this.parent = parent;
            this.size = size;
            this.valuesSource = (ValuesSource.Numeric) config.getValuesSource();
        }

        @Override
        public String name() {
            return name;
        }

        @Override
        public Aggregator parent() {
            return parent;
        }

        @Override
        public Aggregator subAggregator(String aggregatorName) {
            return null;
        }

        @Override
        public InternalAggregation[] buildAggregations(long[] owningBucketOrds) throws IOException {
            InternalAggregation[] internalAggregations = new InternalAggregation[owningBucketOrds.length];
            for (int i = 0; i < internalAggregations.length; i++) {
                PriorityQueue<Long> val = collector.get(owningBucketOrds[i]);
                if (val == null) {
                    internalAggregations[i] = buildEmptyAggregation();
                } else {
                    internalAggregations[i] = new TestInternalAggregation(name(), val);
                }
            }
            return internalAggregations;
        }

        @Override
        public InternalAggregation buildEmptyAggregation() {
            return new TestInternalAggregation(name(), null);
        }

        @Override
        public void close() {}

        @Override
        public LeafBucketCollector getLeafCollector(AggregationExecutionContext aggCtx) throws IOException {
            final NumericDocValues allValues = DocValues.unwrapSingleton(valuesSource.longValues(aggCtx.getLeafReaderContext()));
            final DocsPerOrdIterator docsPerOrdIterator;
            if (aggCtx.getTsid() != null) {
                SortedDocValues tsids = DocValues.getSorted(aggCtx.getLeafReaderContext().reader(), TimeSeriesIdFieldMapper.NAME);
                docsPerOrdIterator = new DocsPerOrdIterator(tsids, size);
            } else {
                docsPerOrdIterator = null;
            }
            return new LeafBucketCollector() {
                @Override
                public DocIdSetIterator competitiveIterator() {
                    return docsPerOrdIterator;
                }

                @Override
                public void collect(int doc, long owningBucketOrd) throws IOException {
                    if (allValues.advanceExact(doc)) {
                        PriorityQueue<Long> queue = collector.get(owningBucketOrd);
                        if (queue == null) {
                            queue = new PriorityQueue<>(size) {
                                @Override
                                protected boolean lessThan(Long a, Long b) {
                                    return a < b;
                                }
                            };
                            collector.put(owningBucketOrd, queue);
                        }
                        queue.insertWithOverflow(allValues.longValue());
                        if (docsPerOrdIterator != null) {
                            docsPerOrdIterator.visitedDoc(doc);
                        }
                    }
                }
            };
        }

        @Override
        public ScoreMode scoreMode() {
            return ScoreMode.COMPLETE_NO_SCORES;
        }

        @Override
        public void preCollection() throws IOException {}

        @Override
        public void postCollection() throws IOException {}

        @Override
        public Aggregator[] subAggregators() {
            throw new UnsupportedOperationException();
        }
    }

    public static class TestInternalAggregation extends InternalAggregation {

        static final String NAME = "test";
        private final List<Long> values;

        protected TestInternalAggregation(String name, PriorityQueue<Long> queue) {
            super(name, null);
            this.values = new ArrayList<>(queue.size());
            for (Long value : queue) {
                values.add(value);
            }
            values.sort((o1, o2) -> Long.compare(o2, o1));
        }

        protected TestInternalAggregation(StreamInput in) throws IOException {
            super(in);
            values = in.readList(StreamInput::readLong);
        }

        @Override
        public String getWriteableName() {
            return NAME;
        }

        @Override
        protected void doWriteTo(StreamOutput out) throws IOException {
            out.writeCollection(values, StreamOutput::writeLong);
        }

        @Override
        public InternalAggregation reduce(List<InternalAggregation> aggregations, AggregationReduceContext reduceContext) {
            return null;
        }

        @Override
        protected boolean mustReduceOnSingleInternalAgg() {
            return false;
        }

        @Override
        public Object getProperty(List<String> path) {
            return null;
        }

        @Override
        public XContentBuilder doXContentBody(XContentBuilder builder, Params params) throws IOException {
            return null;
        }

        @Override
        public int hashCode() {
            return Objects.hash(super.hashCode(), values);
        }

        @Override
        public boolean equals(Object obj) {
            if (this == obj) return true;
            if (obj == null || getClass() != obj.getClass()) return false;
            if (super.equals(obj) == false) return false;
            TestInternalAggregation other = (TestInternalAggregation) obj;
            return Objects.equals(values, other.values);
        }

        @Override
        public String toString() {
            return Arrays.toString(values.toArray(new Long[0]));
        }
    }
}
