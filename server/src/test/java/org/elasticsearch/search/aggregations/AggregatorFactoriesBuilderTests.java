/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.search.aggregations;

import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.io.stream.Writeable.Reader;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.search.SearchModule;
import org.elasticsearch.search.aggregations.AggregatorFactories.Builder;
import org.elasticsearch.search.aggregations.pipeline.CumulativeSumPipelineAggregationBuilder;
import org.elasticsearch.test.AbstractXContentSerializingTestCase;
import org.elasticsearch.xcontent.NamedXContentRegistry;
import org.elasticsearch.xcontent.XContentParser;
import org.junit.Before;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import static java.util.Collections.emptyList;
import static org.hamcrest.Matchers.equalTo;

public class AggregatorFactoriesBuilderTests extends AbstractXContentSerializingTestCase<Builder> {

    private NamedWriteableRegistry namedWriteableRegistry;
    private NamedXContentRegistry namedXContentRegistry;

    @Before
    public void setUp() throws Exception {
        super.setUp();

        // register aggregations as NamedWriteable
        SearchModule searchModule = new SearchModule(Settings.EMPTY, emptyList());
        namedWriteableRegistry = new NamedWriteableRegistry(searchModule.getNamedWriteables());
        namedXContentRegistry = new NamedXContentRegistry(searchModule.getNamedXContents());
    }

    @Override
    protected NamedWriteableRegistry getNamedWriteableRegistry() {
        return namedWriteableRegistry;
    }

    @Override
    protected NamedXContentRegistry xContentRegistry() {
        return namedXContentRegistry;
    }

    @Override
    protected Builder doParseInstance(XContentParser parser) throws IOException {
        // parseAggregators expects to be already inside the xcontent object
        assertThat(parser.nextToken(), equalTo(XContentParser.Token.START_OBJECT));
        AggregatorFactories.Builder builder = AggregatorFactories.parseAggregators(parser);
        return builder;
    }

    @Override
    protected Builder createTestInstance() {
        AggregatorFactories.Builder builder = new AggregatorFactories.Builder();

        // ensure that the unlikely does not happen: 2 aggs share the same name
        Set<String> names = new HashSet<>();
        for (int i = 0; i < randomIntBetween(1, 20); ++i) {
            AggregationBuilder aggBuilder = getRandomAggregation();
            if (names.add(aggBuilder.getName())) {
                builder.addAggregator(aggBuilder);
            }
        }

        for (int i = 0; i < randomIntBetween(0, 20); ++i) {
            PipelineAggregationBuilder aggBuilder = getRandomPipelineAggregation();
            if (names.add(aggBuilder.getName())) {
                builder.addPipelineAggregator(aggBuilder);
            }
        }

        return builder;
    }

    @Override
    protected Builder mutateInstance(Builder instance) {
        return null;// TODO implement https://github.com/elastic/elasticsearch/issues/25929
    }

    @Override
    protected Reader<Builder> instanceReader() {
        return AggregatorFactories.Builder::new;
    }

    public void testUnorderedEqualsSubSet() {
        Set<String> names = new HashSet<>();
        List<AggregationBuilder> aggBuilders = new ArrayList<>();

        while (names.size() < 2) {
            AggregationBuilder aggBuilder = getRandomAggregation();

            if (names.add(aggBuilder.getName())) {
                aggBuilders.add(aggBuilder);
            }
        }

        AggregatorFactories.Builder builder1 = new AggregatorFactories.Builder();
        AggregatorFactories.Builder builder2 = new AggregatorFactories.Builder();

        builder1.addAggregator(aggBuilders.get(0));
        builder1.addAggregator(aggBuilders.get(1));
        builder2.addAggregator(aggBuilders.get(1));

        assertFalse(builder1.equals(builder2));
        assertFalse(builder2.equals(builder1));
        assertNotEquals(builder1.hashCode(), builder2.hashCode());

        builder2.addAggregator(aggBuilders.get(0));
        assertTrue(builder1.equals(builder2));
        assertTrue(builder2.equals(builder1));
        assertEquals(builder1.hashCode(), builder2.hashCode());

        builder1.addPipelineAggregator(getRandomPipelineAggregation());
        assertFalse(builder1.equals(builder2));
        assertFalse(builder2.equals(builder1));
        assertNotEquals(builder1.hashCode(), builder2.hashCode());
    }

    private static AggregationBuilder getRandomAggregation() {
        // just a couple of aggregations, sufficient for the purpose of this test
        final int randomAggregatorPoolSize = 4;
        return switch (randomIntBetween(1, randomAggregatorPoolSize)) {
            case 1 -> AggregationBuilders.avg(randomAlphaOfLengthBetween(3, 10)).field("foo");
            case 2 -> AggregationBuilders.min(randomAlphaOfLengthBetween(3, 10)).field("foo");
            case 3 -> AggregationBuilders.max(randomAlphaOfLengthBetween(3, 10)).field("foo");
            case 4 -> AggregationBuilders.sum(randomAlphaOfLengthBetween(3, 10)).field("foo");
            default ->

                // never reached
                null;
        };

    }

    private static PipelineAggregationBuilder getRandomPipelineAggregation() {
        // just 1 type of pipeline agg, sufficient for the purpose of this test
        String name = randomAlphaOfLengthBetween(3, 20);
        String bucketsPath = randomAlphaOfLengthBetween(3, 20);
        PipelineAggregationBuilder builder = new CumulativeSumPipelineAggregationBuilder(name, bucketsPath);
        return builder;
    }
}
