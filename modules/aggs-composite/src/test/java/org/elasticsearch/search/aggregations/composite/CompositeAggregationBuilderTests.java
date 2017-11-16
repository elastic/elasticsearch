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

package org.elasticsearch.search.aggregations.composite;

import org.elasticsearch.common.Strings;
import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.elasticsearch.common.io.stream.NamedWriteableAwareStreamInput;
import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.NamedXContentRegistry;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.script.Script;
import org.elasticsearch.search.SearchModule;
import org.elasticsearch.search.aggregations.AggregationBuilder;
import org.elasticsearch.search.aggregations.AggregatorFactories;
import org.elasticsearch.search.aggregations.bucket.histogram.DateHistogramInterval;
import org.elasticsearch.search.sort.SortOrder;
import org.elasticsearch.test.ESTestCase;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import static org.elasticsearch.test.EqualsHashCodeTestUtils.checkEqualsAndHashCode;
import static org.hamcrest.Matchers.hasSize;

public class CompositeAggregationBuilderTests extends ESTestCase {
    static final CompositeAggregationPlugin PLUGIN = new CompositeAggregationPlugin();

    @Override
    protected NamedXContentRegistry xContentRegistry() {
        return new NamedXContentRegistry(
            new SearchModule(Settings.EMPTY, false, Collections.singletonList(PLUGIN)).getNamedXContents()
        );
    }

    @Override
    protected NamedWriteableRegistry writableRegistry() {
        return new NamedWriteableRegistry(
            new SearchModule(Settings.EMPTY, false, Collections.singletonList(PLUGIN)).getNamedWriteables()
        );
    }

    private DateHistogramValuesSourceBuilder randomDateHistogramSourceBuilder() {
        DateHistogramValuesSourceBuilder histo = new DateHistogramValuesSourceBuilder(randomAlphaOfLengthBetween(5, 10));
        if (randomBoolean()) {
            histo.field(randomAlphaOfLengthBetween(1, 20));
        } else {
            histo.script(new Script(randomAlphaOfLengthBetween(10, 20)));
        }
        if (randomBoolean()) {
            histo.dateHistogramInterval(randomFrom(DateHistogramInterval.days(10),
                DateHistogramInterval.minutes(1), DateHistogramInterval.weeks(1)));
        } else {
            histo.interval(randomNonNegativeLong());
        }
        if (randomBoolean()) {
            histo.timeZone(randomDateTimeZone());
        }
        return histo;
    }

    private TermsValuesSourceBuilder randomTermsSourceBuilder() {
        TermsValuesSourceBuilder terms = new TermsValuesSourceBuilder(randomAlphaOfLengthBetween(5, 10));
        if (randomBoolean()) {
            terms.field(randomAlphaOfLengthBetween(1, 20));
        } else {
            terms.script(new Script(randomAlphaOfLengthBetween(10, 20)));
        }
        terms.order(randomFrom(SortOrder.values()));
        return terms;
    }

    private HistogramValuesSourceBuilder randomHistogramSourceBuilder() {
        HistogramValuesSourceBuilder histo = new HistogramValuesSourceBuilder(randomAlphaOfLengthBetween(5, 10));
        if (randomBoolean()) {
            histo.field(randomAlphaOfLengthBetween(1, 20));
        } else {
            histo.script(new Script(randomAlphaOfLengthBetween(10, 20)));
        }
        histo.interval(randomDoubleBetween(Math.nextUp(0), Double.MAX_VALUE, false));
        return histo;
    }

    private CompositeAggregationBuilder randomBuilder() {
        int numSources = randomIntBetween(1, 10);
        List<CompositeValuesSourceBuilder<?>> sources = new ArrayList<>();
        for (int i = 0; i < numSources; i++) {
            int type = randomIntBetween(0, 2);
            switch (type) {
                case 0:
                    sources.add(randomTermsSourceBuilder());
                    break;
                case 1:
                    sources.add(randomDateHistogramSourceBuilder());
                    break;
                case 2:
                    sources.add(randomHistogramSourceBuilder());
                    break;
                default:
                    throw new AssertionError("wrong branch");
            }
        }
        return new CompositeAggregationBuilder(randomAlphaOfLength(10), sources);
    }

    public void testFromXContent() throws IOException {
        CompositeAggregationBuilder testAgg = randomBuilder();
        AggregatorFactories.Builder factoriesBuilder = AggregatorFactories.builder().addAggregator(testAgg);
        XContentBuilder builder = XContentFactory.contentBuilder(randomFrom(XContentType.values()));
        if (randomBoolean()) {
            builder.prettyPrint();
        }
        factoriesBuilder.toXContent(builder, ToXContent.EMPTY_PARAMS);
        XContentBuilder shuffled = shuffleXContent(builder);
        XContentParser parser = createParser(shuffled);
        AggregationBuilder newAgg = assertParse(parser);
        assertNotSame(newAgg, testAgg);
        assertEquals(testAgg, newAgg);
        assertEquals(testAgg.hashCode(), newAgg.hashCode());
    }

    public void testToString() throws IOException {
        CompositeAggregationBuilder testAgg = randomBuilder();
        String toString = randomBoolean() ? Strings.toString(testAgg) : testAgg.toString();
        XContentParser parser = createParser(XContentType.JSON.xContent(), toString);
        AggregationBuilder newAgg = assertParse(parser);
        assertNotSame(newAgg, testAgg);
        assertEquals(testAgg, newAgg);
        assertEquals(testAgg.hashCode(), newAgg.hashCode());
    }

    private AggregationBuilder assertParse(XContentParser parser) throws IOException {
        assertSame(XContentParser.Token.START_OBJECT, parser.nextToken());
        AggregatorFactories.Builder parsed = AggregatorFactories.parseAggregators(parser);
        assertThat(parsed.getAggregatorFactories(), hasSize(1));
        assertThat(parsed.getPipelineAggregatorFactories(), hasSize(0));
        AggregationBuilder newAgg = parsed.getAggregatorFactories().get(0);
        assertNull(parser.nextToken());
        assertNotNull(newAgg);
        return newAgg;
    }

    /**
     * Test serialization and deserialization of the test AggregatorFactory.
     */
    public void testSerialization() throws IOException {
        CompositeAggregationBuilder testAgg = randomBuilder();
        try (BytesStreamOutput output = new BytesStreamOutput()) {
            output.writeNamedWriteable(testAgg);
            try (StreamInput in = new NamedWriteableAwareStreamInput(output.bytes().streamInput(), writableRegistry())) {
                AggregationBuilder deserialized = in.readNamedWriteable(AggregationBuilder.class);
                assertEquals(testAgg, deserialized);
                assertEquals(testAgg.hashCode(), deserialized.hashCode());
                assertNotSame(testAgg, deserialized);
            }
        }
    }

    public void testEqualsAndHashcode() throws IOException {
        checkEqualsAndHashCode(randomBuilder(), this::copyAggregation);
    }

    private CompositeAggregationBuilder copyAggregation(CompositeAggregationBuilder agg) throws IOException {
        try (BytesStreamOutput output = new BytesStreamOutput()) {
            agg.writeTo(output);
            try (StreamInput in = new NamedWriteableAwareStreamInput(output.bytes().streamInput(), writableRegistry())) {
                return (CompositeAggregationBuilder) writableRegistry().getReader(AggregationBuilder.class,
                    agg.getWriteableName()).read(in);
            }
        }
    }
}
