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
import org.elasticsearch.env.Environment;
import org.elasticsearch.indices.IndicesModule;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.plugins.PluginsService;
import org.elasticsearch.plugins.SearchPlugin;
import org.elasticsearch.search.SearchModule;
import org.elasticsearch.search.aggregations.support.ValuesSourceAggregationBuilder;
import org.elasticsearch.test.AbstractQueryTestCase;
import org.elasticsearch.test.ESTestCase;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;

import static org.elasticsearch.test.EqualsHashCodeTestUtils.checkEqualsAndHashCode;
import static org.hamcrest.Matchers.hasSize;

public abstract class BaseAggregationTestCase<AB extends AbstractAggregationBuilder<AB>> extends ESTestCase {

    protected static final String STRING_FIELD_NAME = "mapped_string";
    protected static final String INT_FIELD_NAME = "mapped_int";
    protected static final String DOUBLE_FIELD_NAME = "mapped_double";
    protected static final String BOOLEAN_FIELD_NAME = "mapped_boolean";
    protected static final String DATE_FIELD_NAME = "mapped_date";
    protected static final String IP_FIELD_NAME = "mapped_ip";

    private String[] currentTypes;

    protected String[] getCurrentTypes() {
        return currentTypes;
    }

    private NamedWriteableRegistry namedWriteableRegistry;
    private NamedXContentRegistry xContentRegistry;
    protected abstract AB createTestAggregatorBuilder();

    protected Collection<Class<? extends Plugin>> getPlugins() {
        return Collections.emptyList();
    }

    /**
     * Setup for the whole base test class.
     */
    @Override
    public void setUp() throws Exception {
        super.setUp();
        Settings settings = Settings.builder()
            .put("node.name", AbstractQueryTestCase.class.toString())
            .put(Environment.PATH_HOME_SETTING.getKey(), createTempDir())
            .build();
        IndicesModule indicesModule = new IndicesModule(Collections.emptyList());
        PluginsService pluginsService = new PluginsService(settings, null, null, null, getPlugins());
        SearchModule searchModule = new SearchModule(settings, false, pluginsService.filterPlugins(SearchPlugin.class));
        List<NamedWriteableRegistry.Entry> entries = new ArrayList<>();
        entries.addAll(indicesModule.getNamedWriteables());
        entries.addAll(searchModule.getNamedWriteables());
        namedWriteableRegistry = new NamedWriteableRegistry(entries);
        xContentRegistry = new NamedXContentRegistry(searchModule.getNamedXContents());
        //create some random type with some default field, those types will stick around for all of the subclasses
        currentTypes = new String[randomIntBetween(0, 5)];
        for (int i = 0; i < currentTypes.length; i++) {
            String type = randomAlphaOfLengthBetween(1, 10);
            currentTypes[i] = type;
        }
    }

    @Override
    protected NamedXContentRegistry xContentRegistry() {
        return xContentRegistry;
    }

    /**
     * Generic test that creates new AggregatorFactory from the test
     * AggregatorFactory and checks both for equality and asserts equality on
     * the two queries.
     */
    public void testFromXContent() throws IOException {
        AB testAgg = createTestAggregatorBuilder();
        AggregatorFactories.Builder factoriesBuilder = AggregatorFactories.builder().addAggregator(testAgg);
        XContentBuilder builder = XContentFactory.contentBuilder(randomFrom(XContentType.values()));
        if (randomBoolean()) {
            builder.prettyPrint();
        }
        factoriesBuilder.toXContent(builder, ToXContent.EMPTY_PARAMS);
        XContentBuilder shuffled = shuffleXContent(builder);
        XContentParser parser = createParser(shuffled);
        AggregationBuilder newAgg = parse(parser);
        assertNotSame(newAgg, testAgg);
        assertEquals(testAgg, newAgg);
        assertEquals(testAgg.hashCode(), newAgg.hashCode());
    }

    /**
     * Generic test that checks that the toString method renders the XContent
     * correctly.
     */
    public void testToString() throws IOException {
        AB testAgg = createTestAggregatorBuilder();
        String toString = randomBoolean() ? Strings.toString(testAgg) : testAgg.toString();
        XContentParser parser = createParser(XContentType.JSON.xContent(), toString);
        AggregationBuilder newAgg = parse(parser);
        assertNotSame(newAgg, testAgg);
        assertEquals(testAgg, newAgg);
        assertEquals(testAgg.hashCode(), newAgg.hashCode());
    }

    protected AggregationBuilder parse(XContentParser parser) throws IOException {
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
        AB testAgg = createTestAggregatorBuilder();
        try (BytesStreamOutput output = new BytesStreamOutput()) {
            output.writeNamedWriteable(testAgg);
            try (StreamInput in = new NamedWriteableAwareStreamInput(output.bytes().streamInput(), namedWriteableRegistry)) {
                AggregationBuilder deserialized = in.readNamedWriteable(AggregationBuilder.class);
                assertEquals(testAgg, deserialized);
                assertEquals(testAgg.hashCode(), deserialized.hashCode());
                assertNotSame(testAgg, deserialized);
            }
        }
    }

    public void testEqualsAndHashcode() throws IOException {
        // TODO we only change name and boost, we should extend by any sub-test supplying a "mutate" method that randomly changes one
        // aspect of the object under test
        checkEqualsAndHashCode(createTestAggregatorBuilder(), this::copyAggregation);
    }

    // we use the streaming infra to create a copy of the query provided as
    // argument
    private AB copyAggregation(AB agg) throws IOException {
        try (BytesStreamOutput output = new BytesStreamOutput()) {
            agg.writeTo(output);
            try (StreamInput in = new NamedWriteableAwareStreamInput(output.bytes().streamInput(), namedWriteableRegistry)) {
                @SuppressWarnings("unchecked")
                AB secondAgg = (AB) namedWriteableRegistry.getReader(AggregationBuilder.class, agg.getWriteableName()).read(in);
                return secondAgg;
            }
        }
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

    protected void randomFieldOrScript(ValuesSourceAggregationBuilder<?, ?> factory, String field) {
        int choice = randomInt(2);
        switch (choice) {
        case 0:
            factory.field(field);
            break;
        case 1:
            factory.field(field);
            factory.script(mockScript("_value + 1"));
            break;
        case 2:
            factory.script(mockScript("doc[" + field + "] + 1"));
            break;
        default:
            throw new AssertionError("Unknow random operation [" + choice + "]");
        }
    }
}
