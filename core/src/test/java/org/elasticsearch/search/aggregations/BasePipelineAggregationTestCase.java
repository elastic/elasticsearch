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

import org.elasticsearch.cluster.metadata.MetaData;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.ParseFieldMatcher;
import org.elasticsearch.common.inject.Injector;
import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.elasticsearch.common.io.stream.NamedWriteableAwareStreamInput;
import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.query.QueryParseContext;
import org.elasticsearch.indices.query.IndicesQueriesRegistry;
import org.elasticsearch.search.SearchRequestParsers;
import org.elasticsearch.search.aggregations.pipeline.AbstractPipelineAggregationBuilder;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.threadpool.ThreadPool;
import org.junit.AfterClass;
import org.junit.BeforeClass;

import java.io.IOException;
import static org.hamcrest.Matchers.equalTo;

public abstract class BasePipelineAggregationTestCase<AF extends AbstractPipelineAggregationBuilder<AF>> extends ESTestCase {

    protected static final String STRING_FIELD_NAME = "mapped_string";
    protected static final String INT_FIELD_NAME = "mapped_int";
    protected static final String DOUBLE_FIELD_NAME = "mapped_double";
    protected static final String BOOLEAN_FIELD_NAME = "mapped_boolean";
    protected static final String DATE_FIELD_NAME = "mapped_date";

    private static Injector injector;
    private static Index index;

    private static String[] currentTypes;

    protected static String[] getCurrentTypes() {
        return currentTypes;
    }

    private static NamedWriteableRegistry namedWriteableRegistry;

    protected static AggregatorParsers aggParsers;
    protected static ParseFieldMatcher parseFieldMatcher;
    protected static IndicesQueriesRegistry queriesRegistry;

    protected abstract AF createTestAggregatorFactory();

    /**
     * Setup for the whole base test class.
     */
    @BeforeClass
    public static void init() throws IOException {
        index = new Index(randomAsciiOfLengthBetween(1, 10), "_na_");
        injector = BaseAggregationTestCase.buildInjector(index);
        namedWriteableRegistry = injector.getInstance(NamedWriteableRegistry.class);
        aggParsers = injector.getInstance(SearchRequestParsers.class).aggParsers;
        //create some random type with some default field, those types will stick around for all of the subclasses
        currentTypes = new String[randomIntBetween(0, 5)];
        for (int i = 0; i < currentTypes.length; i++) {
            String type = randomAsciiOfLengthBetween(1, 10);
            currentTypes[i] = type;
        }
        queriesRegistry = injector.getInstance(IndicesQueriesRegistry.class);
        parseFieldMatcher = ParseFieldMatcher.STRICT;
    }

    @AfterClass
    public static void afterClass() throws Exception {
        injector.getInstance(ClusterService.class).close();
        terminate(injector.getInstance(ThreadPool.class));
        injector = null;
        index = null;
        aggParsers = null;
        currentTypes = null;
        namedWriteableRegistry = null;
    }

    /**
     * Generic test that creates new AggregatorFactory from the test
     * AggregatorFactory and checks both for equality and asserts equality on
     * the two queries.
     */

    public void testFromXContent() throws IOException {
        AF testAgg = createTestAggregatorFactory();
        AggregatorFactories.Builder factoriesBuilder = AggregatorFactories.builder().skipResolveOrder().addPipelineAggregator(testAgg);
        logger.info("Content string: {}", factoriesBuilder);
        XContentBuilder builder = XContentFactory.contentBuilder(randomFrom(XContentType.values()));
        if (randomBoolean()) {
            builder.prettyPrint();
        }
        factoriesBuilder.toXContent(builder, ToXContent.EMPTY_PARAMS);
        XContentBuilder shuffled = shuffleXContent(builder);
        XContentParser parser = XContentFactory.xContent(shuffled.bytes()).createParser(shuffled.bytes());
        QueryParseContext parseContext = new QueryParseContext(queriesRegistry, parser, parseFieldMatcher);
        String contentString = factoriesBuilder.toString();
        logger.info("Content string: {}", contentString);
        assertSame(XContentParser.Token.START_OBJECT, parser.nextToken());
        assertSame(XContentParser.Token.FIELD_NAME, parser.nextToken());
        assertEquals(testAgg.getName(), parser.currentName());
        assertSame(XContentParser.Token.START_OBJECT, parser.nextToken());
        assertSame(XContentParser.Token.FIELD_NAME, parser.nextToken());
        assertEquals(testAgg.type(), parser.currentName());
        assertSame(XContentParser.Token.START_OBJECT, parser.nextToken());
        PipelineAggregationBuilder newAgg = aggParsers.pipelineParser(testAgg.getWriteableName(), ParseFieldMatcher.STRICT)
                .parse(testAgg.getName(), parseContext);
        assertSame(XContentParser.Token.END_OBJECT, parser.currentToken());
        assertSame(XContentParser.Token.END_OBJECT, parser.nextToken());
        assertSame(XContentParser.Token.END_OBJECT, parser.nextToken());
        assertNull(parser.nextToken());
        assertNotNull(newAgg);
        assertNotSame(newAgg, testAgg);
        assertEquals(testAgg, newAgg);
        assertEquals(testAgg.hashCode(), newAgg.hashCode());
    }

    /**
     * Test serialization and deserialization of the test AggregatorFactory.
     */

    public void testSerialization() throws IOException {
        AF testAgg = createTestAggregatorFactory();
        try (BytesStreamOutput output = new BytesStreamOutput()) {
            output.writeNamedWriteable(testAgg);
            try (StreamInput in = new NamedWriteableAwareStreamInput(output.bytes().streamInput(), namedWriteableRegistry)) {
                PipelineAggregationBuilder deserializedQuery = in.readNamedWriteable(PipelineAggregationBuilder.class);
                assertEquals(deserializedQuery, testAgg);
                assertEquals(deserializedQuery.hashCode(), testAgg.hashCode());
                assertNotSame(deserializedQuery, testAgg);
            }
        }
    }


    public void testEqualsAndHashcode() throws IOException {
        AF firstAgg = createTestAggregatorFactory();
        assertFalse("aggregation is equal to null", firstAgg.equals(null));
        assertFalse("aggregation is equal to incompatible type", firstAgg.equals(""));
        assertTrue("aggregation is not equal to self", firstAgg.equals(firstAgg));
        assertThat("same aggregation's hashcode returns different values if called multiple times", firstAgg.hashCode(),
                equalTo(firstAgg.hashCode()));

        AF secondQuery = copyAggregation(firstAgg);
        assertTrue("aggregation is not equal to self", secondQuery.equals(secondQuery));
        assertTrue("aggregation is not equal to its copy", firstAgg.equals(secondQuery));
        assertTrue("equals is not symmetric", secondQuery.equals(firstAgg));
        assertThat("aggregation copy's hashcode is different from original hashcode", secondQuery.hashCode(), equalTo(firstAgg.hashCode()));

        AF thirdQuery = copyAggregation(secondQuery);
        assertTrue("aggregation is not equal to self", thirdQuery.equals(thirdQuery));
        assertTrue("aggregation is not equal to its copy", secondQuery.equals(thirdQuery));
        assertThat("aggregation copy's hashcode is different from original hashcode", secondQuery.hashCode(),
                equalTo(thirdQuery.hashCode()));
        assertTrue("equals is not transitive", firstAgg.equals(thirdQuery));
        assertThat("aggregation copy's hashcode is different from original hashcode", firstAgg.hashCode(), equalTo(thirdQuery.hashCode()));
        assertTrue("equals is not symmetric", thirdQuery.equals(secondQuery));
        assertTrue("equals is not symmetric", thirdQuery.equals(firstAgg));
    }

    // we use the streaming infra to create a copy of the query provided as
    // argument
    private AF copyAggregation(AF agg) throws IOException {
        try (BytesStreamOutput output = new BytesStreamOutput()) {
            output.writeNamedWriteable(agg);
            try (StreamInput in = new NamedWriteableAwareStreamInput(output.bytes().streamInput(), namedWriteableRegistry)) {
                @SuppressWarnings("unchecked")
                AF secondAgg = (AF) in.readNamedWriteable(PipelineAggregationBuilder.class);
                return secondAgg;
            }
        }
    }

    protected String[] getRandomTypes() {
        String[] types;
        if (currentTypes.length > 0 && randomBoolean()) {
            int numberOfQueryTypes = randomIntBetween(1, currentTypes.length);
            types = new String[numberOfQueryTypes];
            for (int i = 0; i < numberOfQueryTypes; i++) {
                types[i] = randomFrom(currentTypes);
            }
        } else {
            if (randomBoolean()) {
                types = new String[]{MetaData.ALL};
            } else {
                types = new String[0];
            }
        }
        return types;
    }

    public String randomNumericField() {
        int randomInt = randomInt(3);
        switch (randomInt) {
            case 0:
                return DATE_FIELD_NAME;
            case 1:
                return DOUBLE_FIELD_NAME;
            case 2:
            default:
                return INT_FIELD_NAME;
        }
    }
}
