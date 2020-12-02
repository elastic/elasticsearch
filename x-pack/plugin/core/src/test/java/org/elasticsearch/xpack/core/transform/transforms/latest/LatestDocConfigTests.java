/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.core.transform.transforms.latest;

import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.io.stream.Writeable.Reader;
import org.elasticsearch.common.xcontent.DeprecationHandler;
import org.elasticsearch.common.xcontent.LoggingDeprecationHandler;
import org.elasticsearch.common.xcontent.NamedXContentRegistry;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.search.aggregations.bucket.composite.CompositeAggregationBuilder;
import org.elasticsearch.search.aggregations.bucket.composite.CompositeValuesSourceBuilder;
import org.elasticsearch.search.sort.SortBuilders;
import org.elasticsearch.search.sort.SortOrder;
import org.elasticsearch.xpack.core.transform.transforms.AbstractSerializingTransformTestCase;

import java.io.IOException;
import java.util.Collections;
import java.util.stream.Collectors;

import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.nullValue;

public class LatestDocConfigTests extends AbstractSerializingTransformTestCase<LatestDocConfig> {

    public static LatestDocConfig randomLatestDocConfig() {
        return new LatestDocConfig(
            randomList(1, 10, () -> randomAlphaOfLengthBetween(1, 10)),
            Collections.singletonList(SortBuilders.fieldSort(randomAlphaOfLengthBetween(1, 10))));
    }

    @Override
    protected LatestDocConfig doParseInstance(XContentParser parser) throws IOException {
        return LatestDocConfig.fromXContent(parser, false);
    }

    @Override
    protected LatestDocConfig createTestInstance() {
        return randomLatestDocConfig();
    }

    @Override
    protected Reader<LatestDocConfig> instanceReader() {
        return LatestDocConfig::new;
    }

    public void testValidate_ValidConfig_SortAsString() throws IOException {
        String json = "{"
            + " \"unique_key\": [ \"event\" ],"
            + " \"sort\": \"timestamp\""
            + "}";

        LatestDocConfig config = createLatestDocConfigFromString(json, false);
        assertThat(config.validate(null), is(nullValue()));
        assertThat(config.getUniqueKey(), is(equalTo(Collections.singletonList("event"))));
        assertThat(config.getSort(), is(equalTo(Collections.singletonList(SortBuilders.fieldSort("timestamp")))));
    }

    public void testValidate_ValidConfig_SortAsStringArray() throws IOException {
        String json = "{"
            + " \"unique_key\": [ \"event\" ],"
            + " \"sort\": [ \"timestamp\" ]"
            + "}";

        LatestDocConfig config = createLatestDocConfigFromString(json, false);
        assertThat(config.validate(null), is(nullValue()));
        assertThat(config.getUniqueKey(), is(equalTo(Collections.singletonList("event"))));
        assertThat(config.getSort(), is(equalTo(Collections.singletonList(SortBuilders.fieldSort("timestamp")))));
    }

    public void testValidate_ValidConfig_SortAsObject_DefaultOrder() throws IOException {
        String json = "{"
            + " \"unique_key\": [ \"event\" ],"
            + " \"sort\": { \"timestamp\": {} }"
            + "}";

        LatestDocConfig config = createLatestDocConfigFromString(json, false);
        assertThat(config.validate(null), is(nullValue()));
        assertThat(config.getUniqueKey(), is(equalTo(Collections.singletonList("event"))));
        assertThat(config.getSort(), is(equalTo(Collections.singletonList(SortBuilders.fieldSort("timestamp")))));
    }

    public void testValidate_ValidConfig_SortAsObject_ExplicitOrder() throws IOException {
        String json = "{"
            + " \"unique_key\": [ \"event\" ],"
            + " \"sort\": { \"timestamp\": { \"order\": \"desc\" } }"
            + "}";

        LatestDocConfig config = createLatestDocConfigFromString(json, false);
        assertThat(config.validate(null), is(nullValue()));
        assertThat(config.getUniqueKey(), is(equalTo(Collections.singletonList("event"))));
        assertThat(config.getSort(), is(equalTo(Collections.singletonList(SortBuilders.fieldSort("timestamp").order(SortOrder.DESC)))));
    }

    public void testValidate_EmptyUniqueKey() throws IOException {
        String json = "{"
            + " \"unique_key\": [],"
            + " \"sort\": { \"timestamp\": { \"order\": \"desc\" } }"
            + "}";

        LatestDocConfig config = createLatestDocConfigFromString(json, false);
        assertThat(config.validate(null).validationErrors(), contains("latest_doc.unique_key must be non-empty"));
    }

    public void testValidate_EmptyUniqueKeyElement() throws IOException {
        String json = "{"
            + " \"unique_key\": [ \"event1\", \"event2\", \"\", \"event3\" ],"
            + " \"sort\": { \"timestamp\": { \"order\": \"desc\" } }"
            + "}";

        LatestDocConfig config = createLatestDocConfigFromString(json, false);
        assertThat(config.validate(null).validationErrors(), contains("latest_doc.unique_key[2] element must be non-empty"));
    }

    public void testValidate_EmptySort() throws IOException {
        String json = "{"
            + " \"unique_key\": [ \"event\" ],"
            + " \"sort\": []"
            + "}";

        LatestDocConfig config = createLatestDocConfigFromString(json, false);
        assertThat(config.validate(null).validationErrors(), contains("latest_doc.sort must have exactly one element"));
    }

    public void testValidate_TooManySortCriteria() throws IOException {
        String json = "{"
            + " \"unique_key\": [ \"event\" ],"
            + " \"sort\": [ \"timestamp\", \"event\" ]"
            + "}";

        LatestDocConfig config = createLatestDocConfigFromString(json, false);
        assertThat(config.validate(null).validationErrors(), contains("latest_doc.sort must have exactly one element"));
    }

    public void testValidate_InvalidSortType() throws IOException {
        String json = "{"
            + " \"unique_key\": [ \"event\" ],"
            + " \"sort\": { \"_script\": { \"script\": { \"source\": \"\" }, \"type\": \"string\" } }"
            + "}";

        LatestDocConfig config = createLatestDocConfigFromString(json, false);
        assertThat(
            config.validate(null).validationErrors(),
            contains("latest_doc.sort[0] must be sorting based on a document field, was: _script"));
    }

    public void testToCompositeAggXContent() throws IOException {
        String json = "{"
            + " \"unique_key\": [ \"field-A\", \"field-B\", \"field-C\", \"field-D\", \"field-E\" ],"
            + " \"sort\": \"timestamp\""
            + "}";

        LatestDocConfig config = createLatestDocConfigFromString(json, false);
        try (XContentBuilder builder = jsonBuilder()) {
            config.toCompositeAggXContent(builder);
            XContentParser parser = builder.generator()
                .contentType()
                .xContent()
                .createParser(NamedXContentRegistry.EMPTY, LoggingDeprecationHandler.INSTANCE, BytesReference.bytes(builder).streamInput());
            CompositeAggregationBuilder compositeAggregation = CompositeAggregationBuilder.PARSER.parse(parser, "dummy");
            assertThat(
                compositeAggregation.sources().stream().map(CompositeValuesSourceBuilder::field).collect(Collectors.toList()),
                containsInAnyOrder("field-A", "field-B", "field-C", "field-D", "field-E"));
        }
    }

    private LatestDocConfig createLatestDocConfigFromString(String json, boolean lenient) throws IOException {
        final XContentParser parser = XContentType.JSON.xContent()
            .createParser(xContentRegistry(), DeprecationHandler.THROW_UNSUPPORTED_OPERATION, json);
        return LatestDocConfig.fromXContent(parser, lenient);
    }
}
