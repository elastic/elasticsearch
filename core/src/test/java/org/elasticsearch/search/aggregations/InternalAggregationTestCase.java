/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.elasticsearch.search.aggregations;

import org.elasticsearch.common.ParseField;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.MockBigArrays;
import org.elasticsearch.common.xcontent.ContextParser;
import org.elasticsearch.common.xcontent.NamedXContentRegistry;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.indices.breaker.NoneCircuitBreakerService;
import org.elasticsearch.rest.action.search.RestSearchAction;
import org.elasticsearch.script.ScriptService;
import org.elasticsearch.search.DocValueFormat;
import org.elasticsearch.search.SearchModule;
import org.elasticsearch.search.aggregations.metrics.avg.AvgAggregationBuilder;
import org.elasticsearch.search.aggregations.metrics.avg.ParsedAvg;
import org.elasticsearch.search.aggregations.metrics.cardinality.CardinalityAggregationBuilder;
import org.elasticsearch.search.aggregations.metrics.cardinality.ParsedCardinality;
import org.elasticsearch.search.aggregations.metrics.max.MaxAggregationBuilder;
import org.elasticsearch.search.aggregations.metrics.max.ParsedMax;
import org.elasticsearch.search.aggregations.metrics.min.MinAggregationBuilder;
import org.elasticsearch.search.aggregations.metrics.min.ParsedMin;
import org.elasticsearch.search.aggregations.metrics.percentiles.hdr.InternalHDRPercentileRanks;
import org.elasticsearch.search.aggregations.metrics.percentiles.hdr.InternalHDRPercentiles;
import org.elasticsearch.search.aggregations.metrics.percentiles.hdr.ParsedHDRPercentileRanks;
import org.elasticsearch.search.aggregations.metrics.percentiles.hdr.ParsedHDRPercentiles;
import org.elasticsearch.search.aggregations.metrics.percentiles.tdigest.InternalTDigestPercentileRanks;
import org.elasticsearch.search.aggregations.metrics.percentiles.tdigest.InternalTDigestPercentiles;
import org.elasticsearch.search.aggregations.metrics.percentiles.tdigest.ParsedTDigestPercentileRanks;
import org.elasticsearch.search.aggregations.metrics.percentiles.tdigest.ParsedTDigestPercentiles;
import org.elasticsearch.search.aggregations.metrics.stats.ParsedStats;
import org.elasticsearch.search.aggregations.metrics.stats.StatsAggregationBuilder;
import org.elasticsearch.search.aggregations.metrics.stats.extended.ExtendedStatsAggregationBuilder;
import org.elasticsearch.search.aggregations.metrics.stats.extended.ParsedExtendedStats;
import org.elasticsearch.search.aggregations.metrics.sum.ParsedSum;
import org.elasticsearch.search.aggregations.metrics.sum.SumAggregationBuilder;
import org.elasticsearch.search.aggregations.metrics.valuecount.ParsedValueCount;
import org.elasticsearch.search.aggregations.metrics.valuecount.ValueCountAggregationBuilder;
import org.elasticsearch.search.aggregations.pipeline.InternalSimpleValue;
import org.elasticsearch.search.aggregations.pipeline.ParsedSimpleValue;
import org.elasticsearch.search.aggregations.pipeline.PipelineAggregator;
import org.elasticsearch.search.aggregations.pipeline.bucketmetrics.InternalBucketMetricValue;
import org.elasticsearch.search.aggregations.pipeline.bucketmetrics.ParsedBucketMetricValue;
import org.elasticsearch.search.aggregations.pipeline.bucketmetrics.percentile.ParsedPercentilesBucket;
import org.elasticsearch.search.aggregations.pipeline.bucketmetrics.percentile.PercentilesBucketPipelineAggregationBuilder;
import org.elasticsearch.search.aggregations.pipeline.bucketmetrics.stats.ParsedStatsBucket;
import org.elasticsearch.search.aggregations.pipeline.bucketmetrics.stats.StatsBucketPipelineAggregationBuilder;
import org.elasticsearch.search.aggregations.pipeline.bucketmetrics.stats.extended.ExtendedStatsBucketPipelineAggregationBuilder;
import org.elasticsearch.search.aggregations.pipeline.bucketmetrics.stats.extended.ParsedExtendedStatsBucket;
import org.elasticsearch.search.aggregations.pipeline.derivative.DerivativePipelineAggregationBuilder;
import org.elasticsearch.search.aggregations.pipeline.derivative.ParsedDerivative;
import org.elasticsearch.test.AbstractWireSerializingTestCase;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Supplier;
import java.util.stream.Collectors;

import static java.util.Collections.emptyList;
import static java.util.Collections.singletonMap;
import static org.elasticsearch.common.xcontent.XContentHelper.toXContent;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertToXContentEquivalent;

public abstract class InternalAggregationTestCase<T extends InternalAggregation> extends AbstractWireSerializingTestCase<T> {

    private final NamedWriteableRegistry namedWriteableRegistry = new NamedWriteableRegistry(
            new SearchModule(Settings.EMPTY, false, emptyList()).getNamedWriteables());

    private final NamedXContentRegistry namedXContentRegistry = new NamedXContentRegistry(getNamedXContents());

    static List<NamedXContentRegistry.Entry> getNamedXContents() {
        Map<String, ContextParser<Object, ? extends Aggregation>> namedXContents = new HashMap<>();
        namedXContents.put(CardinalityAggregationBuilder.NAME, (p, c) -> ParsedCardinality.fromXContent(p, (String) c));
        namedXContents.put(InternalHDRPercentiles.NAME, (p, c) -> ParsedHDRPercentiles.fromXContent(p, (String) c));
        namedXContents.put(InternalHDRPercentileRanks.NAME, (p, c) -> ParsedHDRPercentileRanks.fromXContent(p, (String) c));
        namedXContents.put(InternalTDigestPercentiles.NAME, (p, c) -> ParsedTDigestPercentiles.fromXContent(p, (String) c));
        namedXContents.put(InternalTDigestPercentileRanks.NAME, (p, c) -> ParsedTDigestPercentileRanks.fromXContent(p, (String) c));
        namedXContents.put(PercentilesBucketPipelineAggregationBuilder.NAME, (p, c) -> ParsedPercentilesBucket.fromXContent(p, (String) c));
        namedXContents.put(MinAggregationBuilder.NAME, (p, c) -> ParsedMin.fromXContent(p, (String) c));
        namedXContents.put(MaxAggregationBuilder.NAME, (p, c) -> ParsedMax.fromXContent(p, (String) c));
        namedXContents.put(SumAggregationBuilder.NAME, (p, c) -> ParsedSum.fromXContent(p, (String) c));
        namedXContents.put(AvgAggregationBuilder.NAME, (p, c) -> ParsedAvg.fromXContent(p, (String) c));
        namedXContents.put(ValueCountAggregationBuilder.NAME, (p, c) -> ParsedValueCount.fromXContent(p, (String) c));
        namedXContents.put(InternalSimpleValue.NAME, (p, c) -> ParsedSimpleValue.fromXContent(p, (String) c));
        namedXContents.put(DerivativePipelineAggregationBuilder.NAME, (p, c) -> ParsedDerivative.fromXContent(p, (String) c));
        namedXContents.put(InternalBucketMetricValue.NAME, (p, c) -> ParsedBucketMetricValue.fromXContent(p, (String) c));
        namedXContents.put(StatsAggregationBuilder.NAME, (p, c) -> ParsedStats.fromXContent(p, (String) c));
        namedXContents.put(StatsBucketPipelineAggregationBuilder.NAME, (p, c) -> ParsedStatsBucket.fromXContent(p, (String) c));
        namedXContents.put(ExtendedStatsAggregationBuilder.NAME, (p, c) -> ParsedExtendedStats.fromXContent(p, (String) c));
        namedXContents.put(ExtendedStatsBucketPipelineAggregationBuilder.NAME, (p, c) -> ParsedExtendedStatsBucket.fromXContent(p, (String) c));

        return namedXContents.entrySet().stream()
                .map(entry -> new NamedXContentRegistry.Entry(Aggregation.class, new ParseField(entry.getKey()), entry.getValue()))
                .collect(Collectors.toList());
    }

    protected abstract T createTestInstance(String name, List<PipelineAggregator> pipelineAggregators, Map<String, Object> metaData);

    /** Return an instance on an unmapped field. */
    protected T createUnmappedInstance(String name,
            List<PipelineAggregator> pipelineAggregators,
            Map<String, Object> metaData) {
        // For most impls, we use the same instance in the unmapped case and in the mapped case
        return createTestInstance(name, pipelineAggregators, metaData);
    }

    public void testReduceRandom() {
        String name = randomAlphaOfLength(5);
        List<T> inputs = new ArrayList<>();
        List<InternalAggregation> toReduce = new ArrayList<>();
        int toReduceSize = between(1, 200);
        for (int i = 0; i < toReduceSize; i++) {
            T t = randomBoolean() ? createUnmappedInstance(name) : createTestInstance(name);
            inputs.add(t);
            toReduce.add(t);
        }
        ScriptService mockScriptService = mockScriptService();
        MockBigArrays bigArrays = new MockBigArrays(Settings.EMPTY, new NoneCircuitBreakerService());
        if (randomBoolean() && toReduce.size() > 1) {
            // sometimes do an incremental reduce
            Collections.shuffle(toReduce, random());
            int r = randomIntBetween(1, toReduceSize);
            List<InternalAggregation> internalAggregations = toReduce.subList(0, r);
            InternalAggregation.ReduceContext context =
                new InternalAggregation.ReduceContext(bigArrays, mockScriptService, false);
            @SuppressWarnings("unchecked")
            T reduced = (T) inputs.get(0).reduce(internalAggregations, context);
            toReduce = new ArrayList<>(toReduce.subList(r, toReduceSize));
            toReduce.add(reduced);
        }
        InternalAggregation.ReduceContext context =
            new InternalAggregation.ReduceContext(bigArrays, mockScriptService, true);
        @SuppressWarnings("unchecked")
        T reduced = (T) inputs.get(0).reduce(toReduce, context);
        assertReduced(reduced, inputs);
    }

    /**
     * overwrite in tests that need it
     */
    protected ScriptService mockScriptService() {
        return null;
    }

    protected abstract void assertReduced(T reduced, List<T> inputs);

    @Override
    protected final T createTestInstance() {
        return createTestInstance(randomAlphaOfLength(5));
    }

    private T createTestInstance(String name) {
        List<PipelineAggregator> pipelineAggregators = new ArrayList<>();
        // TODO populate pipelineAggregators
        Map<String, Object> metaData = null;
        if (randomBoolean()) {
            metaData = new HashMap<>();
            int metaDataCount = between(0, 10);
            while (metaData.size() < metaDataCount) {
                metaData.put(randomAlphaOfLength(5), randomAlphaOfLength(5));
            }
        }
        return createTestInstance(name, pipelineAggregators, metaData);
    }

    /** Return an instance on an unmapped field. */
    protected final T createUnmappedInstance(String name) {
        List<PipelineAggregator> pipelineAggregators = new ArrayList<>();
        // TODO populate pipelineAggregators
        Map<String, Object> metaData = new HashMap<>();
        int metaDataCount = randomBoolean() ? 0 : between(1, 10);
        while (metaData.size() < metaDataCount) {
            metaData.put(randomAlphaOfLength(5), randomAlphaOfLength(5));
        }
        return createUnmappedInstance(name, pipelineAggregators, metaData);
    }

    @Override
    protected NamedWriteableRegistry getNamedWriteableRegistry() {
        return namedWriteableRegistry;
    }

    @Override
    protected NamedXContentRegistry xContentRegistry() {
        return namedXContentRegistry;
    }

    public final void testFromXContent() throws IOException {
        final T aggregation = createTestInstance();

        //norelease Remove this assumption when all aggregations can be parsed back.
        assumeTrue("This test does not support the aggregation type yet",
                getNamedXContents().stream().filter(entry -> entry.name.match(aggregation.getType())).count() > 0);

        final Aggregation parsedAggregation = parseAndAssert(aggregation, randomBoolean());
        assertFromXContent(aggregation, (ParsedAggregation) parsedAggregation);
    }

    //norelease TODO make abstract
    protected void assertFromXContent(T aggregation, ParsedAggregation parsedAggregation) {
    }

    @SuppressWarnings("unchecked")
    protected <P extends ParsedAggregation> P parseAndAssert(final InternalAggregation aggregation,
                                                             final boolean shuffled) throws IOException {

        final ToXContent.Params params = new ToXContent.MapParams(singletonMap(RestSearchAction.TYPED_KEYS_PARAM, "true"));
        final XContentType xContentType = randomFrom(XContentType.values());
        final boolean humanReadable = randomBoolean();

        final BytesReference originalBytes;
        if (shuffled) {
            originalBytes = toShuffledXContent(aggregation, xContentType, params, humanReadable);
        } else {
            originalBytes = toXContent(aggregation, xContentType, params, humanReadable);
        }

        Aggregation parsedAggregation;
        try (XContentParser parser = createParser(xContentType.xContent(), originalBytes)) {
            assertEquals(XContentParser.Token.START_OBJECT, parser.nextToken());
            assertEquals(XContentParser.Token.FIELD_NAME, parser.nextToken());

            String currentName = parser.currentName();
            int i = currentName.indexOf(InternalAggregation.TYPED_KEYS_DELIMITER);
            String aggType = currentName.substring(0, i);
            String aggName = currentName.substring(i + 1);

            parsedAggregation = parser.namedObject(Aggregation.class, aggType, aggName);

            assertEquals(XContentParser.Token.END_OBJECT, parser.currentToken());
            assertEquals(XContentParser.Token.END_OBJECT, parser.nextToken());
            assertNull(parser.nextToken());

            assertEquals(aggregation.getName(), parsedAggregation.getName());
            assertEquals(aggregation.getMetaData(), parsedAggregation.getMetaData());

            assertTrue(parsedAggregation instanceof ParsedAggregation);
            assertEquals(aggregation.getType(), ((ParsedAggregation) parsedAggregation).getType());
        }

        BytesReference parsedBytes = toXContent((ToXContent) parsedAggregation, xContentType, params, humanReadable);
        assertToXContentEquivalent(originalBytes, parsedBytes, xContentType);

        return (P) parsedAggregation;
    }

    /**
     * @return a random {@link DocValueFormat} that can be used in aggregations which
     * compute numbers.
     */
    protected static DocValueFormat randomNumericDocValueFormat() {
        final List<Supplier<DocValueFormat>> formats = new ArrayList<>(3);
        formats.add(() -> DocValueFormat.RAW);
        formats.add(() -> new DocValueFormat.Decimal(randomFrom("###.##", "###,###.##")));
        return randomFrom(formats).get();
    }
}
