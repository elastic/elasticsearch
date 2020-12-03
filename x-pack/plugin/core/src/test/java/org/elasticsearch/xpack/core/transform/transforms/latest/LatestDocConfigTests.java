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
import java.util.stream.Collectors;

import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.nullValue;

public class LatestDocConfigTests extends AbstractSerializingTransformTestCase<LatestDocConfig> {

    public static LatestDocConfig randomLatestDocConfig() {
        return new LatestDocConfig(randomList(1, 10, () -> randomAlphaOfLengthBetween(1, 10)), randomAlphaOfLengthBetween(1, 10));
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

    public void testValidate_ValidConfig() throws IOException {
        String json = "{"
            + " \"unique_key\": [ \"event1\", \"event2\", \"event3\" ],"
            + " \"sort\": \"timestamp\""
            + "}";

        LatestDocConfig config = createLatestDocConfigFromString(json);
        assertThat(config.validate(null), is(nullValue()));
        assertThat(config.getUniqueKey(), contains("event1", "event2", "event3"));
        assertThat(config.getSort(), is(equalTo("timestamp")));
        assertThat(config.getSorts(), contains(SortBuilders.fieldSort("timestamp").order(SortOrder.DESC)));
    }

    public void testValidate_EmptyUniqueKey() throws IOException {
        String json = "{"
            + " \"unique_key\": [],"
            + " \"sort\": \"timestamp\""
            + "}";

        LatestDocConfig config = createLatestDocConfigFromString(json);
        assertThat(config.validate(null).validationErrors(), contains("latest.unique_key must be non-empty"));
    }

    public void testValidate_EmptyUniqueKeyElement() throws IOException {
        String json = "{"
            + " \"unique_key\": [ \"event1\", \"\", \"event2\", \"\", \"event3\" ],"
            + " \"sort\": \"timestamp\""
            + "}";

        LatestDocConfig config = createLatestDocConfigFromString(json);
        assertThat(
            config.validate(null).validationErrors(),
            containsInAnyOrder("latest.unique_key[1] element must be non-empty", "latest.unique_key[3] element must be non-empty"));
    }

    public void testValidate_EmptySort() throws IOException {
        String json = "{"
            + " \"unique_key\": [ \"event1\", \"event2\", \"event3\" ],"
            + " \"sort\": \"\""
            + "}";

        LatestDocConfig config = createLatestDocConfigFromString(json);
        assertThat(config.validate(null).validationErrors(), contains("latest.sort must be non-empty"));
    }

    public void testValidate_EmptyUniqueKeyAndSort() throws IOException {
        String json = "{"
            + " \"unique_key\": [],"
            + " \"sort\": \"\""
            + "}";

        LatestDocConfig config = createLatestDocConfigFromString(json);
        assertThat(
            config.validate(null).validationErrors(),
            containsInAnyOrder("latest.unique_key must be non-empty", "latest.sort must be non-empty"));
    }

    public void testToCompositeAggXContent() throws IOException {
        String json = "{"
            + " \"unique_key\": [ \"event1\", \"event2\", \"event3\", \"event4\", \"event5\" ],"
            + " \"sort\": \"timestamp\""
            + "}";

        LatestDocConfig config = createLatestDocConfigFromString(json);
        try (XContentBuilder builder = jsonBuilder()) {
            config.toCompositeAggXContent(builder);
            XContentParser parser = builder.generator()
                .contentType()
                .xContent()
                .createParser(NamedXContentRegistry.EMPTY, LoggingDeprecationHandler.INSTANCE, BytesReference.bytes(builder).streamInput());
            CompositeAggregationBuilder compositeAggregation = CompositeAggregationBuilder.PARSER.parse(parser, "dummy");
            assertThat(
                compositeAggregation.sources().stream().map(CompositeValuesSourceBuilder::field).collect(Collectors.toList()),
                containsInAnyOrder("event1", "event2", "event3", "event4", "event5"));
        }
    }

    private LatestDocConfig createLatestDocConfigFromString(String json) throws IOException {
        final XContentParser parser = XContentType.JSON.xContent()
            .createParser(xContentRegistry(), DeprecationHandler.THROW_UNSUPPORTED_OPERATION, json);
        return LatestDocConfig.fromXContent(parser, false);
    }
}
