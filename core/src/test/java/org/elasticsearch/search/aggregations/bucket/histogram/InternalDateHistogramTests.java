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

package org.elasticsearch.search.aggregations.bucket.histogram;

import org.elasticsearch.common.ParseField;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.joda.Joda;
import org.elasticsearch.common.xcontent.NamedXContentRegistry;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.XContentLocation;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.common.xcontent.XContentParserUtils;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.rest.action.search.RestSearchAction;
import org.elasticsearch.search.DocValueFormat;
import org.elasticsearch.search.aggregations.InternalAggregation;
import org.elasticsearch.search.aggregations.InternalAggregationTestCase;
import org.elasticsearch.search.aggregations.InternalAggregations;
import org.elasticsearch.search.aggregations.pipeline.PipelineAggregator;
import org.joda.time.DateTime;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.function.Supplier;

import static java.util.Collections.emptyMap;
import static java.util.Collections.singletonList;
import static java.util.Collections.singletonMap;
import static org.elasticsearch.common.unit.TimeValue.timeValueHours;
import static org.elasticsearch.common.unit.TimeValue.timeValueMinutes;
import static org.elasticsearch.common.unit.TimeValue.timeValueSeconds;
import static org.elasticsearch.common.xcontent.XContentHelper.toXContent;
import static org.elasticsearch.common.xcontent.XContentParserUtils.ensureExpectedToken;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertToXContentEquivalent;

public class InternalDateHistogramTests extends InternalAggregationTestCase<InternalDateHistogram> {

    @Override
    protected InternalDateHistogram createTestInstance(String name, List<PipelineAggregator> pipelineAggregators,
                                                       Map<String, Object> metaData) {
        return createAggregation(name, pipelineAggregators, metaData, true);
    }

    private InternalDateHistogram createAggregation(String name, List<PipelineAggregator> pipelineAggregators, Map<String, Object> metaData, boolean allowSubAggregations) {
        final boolean keyed = false;// Parsing methods does not work with keyed agg yet.
        DocValueFormat format = randomFrom(DocValueFormat.RAW, new DocValueFormat.DateTime(Joda.forPattern(randomFrom("yyyy/MM/dd HH:mm:ss", "dateOptionalTime")), randomDateTimeZone()));
        long startingDate = System.currentTimeMillis();

        long interval = randomIntBetween(1, 3);
        long intervalMillis = randomFrom(timeValueSeconds(interval), timeValueMinutes(interval), timeValueHours(interval)).getMillis();

        final int subAggs = allowSubAggregations ? randomIntBetween(1, 3) : 0;

        int nbBuckets = randomInt(10);
        List<InternalDateHistogram.Bucket> buckets = new ArrayList<>(nbBuckets);
        for (int i = 0; i < nbBuckets; i++) {
            long key = startingDate + (intervalMillis * i);

            InternalAggregations aggregations;
            if (subAggs > 0) {
                List<InternalAggregation> subAggregations = new ArrayList<>(subAggs);
                for (int j = 0; j < subAggs; j++) {
                    subAggregations.add(createAggregation(name + "_" + i + "_" + j, pipelineAggregators, emptyMap(), false));
                }
                aggregations = new InternalAggregations(subAggregations);
            } else {
                aggregations = InternalAggregations.EMPTY;
            }
            buckets.add(i, new InternalDateHistogram.Bucket(key, randomIntBetween(1, 100), keyed, format, aggregations));
        }

        InternalOrder order = (InternalOrder) randomFrom(InternalHistogram.Order.KEY_ASC,
                InternalHistogram.Order.KEY_DESC, InternalHistogram.Order.COUNT_ASC, InternalHistogram.Order.COUNT_DESC);
        return new InternalDateHistogram(name, buckets, order, 1, 0L, null, format, keyed, pipelineAggregators, metaData);
    }

    @Override
    protected void assertReduced(InternalDateHistogram reduced, List<InternalDateHistogram> inputs) {
        Map<Long, Long> expectedCounts = new TreeMap<>();
        for (Histogram histogram : inputs) {
            for (Histogram.Bucket bucket : histogram.getBuckets()) {
                expectedCounts.compute(((DateTime) bucket.getKey()).getMillis(),
                        (key, oldValue) -> (oldValue == null ? 0 : oldValue) + bucket.getDocCount());
            }
        }
        Map<Long, Long> actualCounts = new TreeMap<>();
        for (Histogram.Bucket bucket : reduced.getBuckets()) {
            actualCounts.compute(((DateTime) bucket.getKey()).getMillis(),
                    (key, oldValue) -> (oldValue == null ? 0 : oldValue) + bucket.getDocCount());
        }
        assertEquals(expectedCounts, actualCounts);
    }

    @Override
    protected Writeable.Reader<InternalDateHistogram> instanceReader() {
        return InternalDateHistogram::new;
    }

    public void testFromXContent() throws IOException {
        final ToXContent.Params params =
                new ToXContent.MapParams(singletonMap(RestSearchAction.TYPED_KEYS_PARAM, "true"));
        final XContentType xContentType = randomFrom(XContentType.values());
        final boolean humanReadable = randomBoolean();

        InternalDateHistogram aggregation = createTestInstance();
        BytesReference originalBytes = toXContent(aggregation, xContentType, params, humanReadable);

        if (randomBoolean()) {
            try (XContentParser parser = createParser(xContentType.xContent(), originalBytes)) {
                originalBytes = shuffleXContent(parser, randomBoolean()).bytes();
            }
        }

        @SuppressWarnings("unchecked")
        NamedXContentRegistry.Entry entry = new NamedXContentRegistry.Entry(
                InternalAggregation.class, new ParseField(aggregation.getWriteableName()),
                (parser, context) -> InternalDateHistogram.fromXContent(parser, (String) context));

        NamedXContentRegistry registry = new NamedXContentRegistry(singletonList(entry));

        InternalAggregation parsedAggregation;
        try (XContentParser parser = xContentType.xContent().createParser(registry, originalBytes)) {
            Supplier<XContentLocation> location = parser::getTokenLocation;
            ensureExpectedToken(XContentParser.Token.START_OBJECT, parser.nextToken(), location);
            ensureExpectedToken(XContentParser.Token.FIELD_NAME, parser.nextToken(), location);

            parsedAggregation = XContentParserUtils.parseTypedKeysObject(parser,
                    InternalAggregation.TYPED_KEYS_DELIMITER, InternalAggregation.class);

            assertEquals(XContentParser.Token.END_OBJECT, parser.nextToken());
            assertNull(parser.nextToken());
        }
        assertTrue(parsedAggregation instanceof InternalDateHistogram);
        assertEquals(aggregation, parsedAggregation);

        BytesReference finalBytes = toXContent(parsedAggregation, xContentType, params, humanReadable);
        assertToXContentEquivalent(originalBytes, finalBytes, xContentType);
    }
}
