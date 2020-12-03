/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.core.transform.transforms.latest;

import org.elasticsearch.common.io.stream.Writeable.Reader;
import org.elasticsearch.common.xcontent.DeprecationHandler;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.search.sort.SortBuilders;
import org.elasticsearch.search.sort.SortOrder;
import org.elasticsearch.xpack.core.transform.transforms.AbstractSerializingTransformTestCase;

import java.io.IOException;

import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.nullValue;

public class LatestDocConfigTests extends AbstractSerializingTransformTestCase<LatestDocConfig> {

    public static LatestDocConfig randomLatestConfig() {
        return new LatestDocConfig(randomList(1, 10, () -> randomAlphaOfLengthBetween(1, 10)), randomAlphaOfLengthBetween(1, 10));
    }

    @Override
    protected LatestDocConfig doParseInstance(XContentParser parser) throws IOException {
        return LatestDocConfig.fromXContent(parser, false);
    }

    @Override
    protected LatestDocConfig createTestInstance() {
        return randomLatestConfig();
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

        LatestDocConfig config = createLatestConfigFromString(json);
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

        LatestDocConfig config = createLatestConfigFromString(json);
        assertThat(config.validate(null).validationErrors(), contains("latest.unique_key must be non-empty"));
    }

    public void testValidate_EmptyUniqueKeyElement() throws IOException {
        String json = "{"
            + " \"unique_key\": [ \"event1\", \"\", \"event2\", \"\", \"event3\" ],"
            + " \"sort\": \"timestamp\""
            + "}";

        LatestDocConfig config = createLatestConfigFromString(json);
        assertThat(
            config.validate(null).validationErrors(),
            containsInAnyOrder("latest.unique_key[1] element must be non-empty", "latest.unique_key[3] element must be non-empty"));
    }

    public void testValidate_DuplicateUniqueKeyElement() throws IOException {
        String json = "{"
            + " \"unique_key\": [ \"event1\", \"event2\", \"event1\" ],"
            + " \"sort\": \"timestamp\""
            + "}";

        LatestDocConfig config = createLatestConfigFromString(json);
        assertThat(
            config.validate(null).validationErrors(),
            containsInAnyOrder("latest.unique_key elements must be unique, found duplicate element [event1]"));
    }

    public void testValidate_EmptySort() throws IOException {
        String json = "{"
            + " \"unique_key\": [ \"event1\", \"event2\", \"event3\" ],"
            + " \"sort\": \"\""
            + "}";

        LatestDocConfig config = createLatestConfigFromString(json);
        assertThat(config.validate(null).validationErrors(), contains("latest.sort must be non-empty"));
    }

    public void testValidate_EmptyUniqueKeyAndSort() throws IOException {
        String json = "{"
            + " \"unique_key\": [],"
            + " \"sort\": \"\""
            + "}";

        LatestDocConfig config = createLatestConfigFromString(json);
        assertThat(
            config.validate(null).validationErrors(),
            containsInAnyOrder("latest.unique_key must be non-empty", "latest.sort must be non-empty"));
    }

    private LatestDocConfig createLatestConfigFromString(String json) throws IOException {
        final XContentParser parser = XContentType.JSON.xContent()
            .createParser(xContentRegistry(), DeprecationHandler.THROW_UNSUPPORTED_OPERATION, json);
        return LatestDocConfig.fromXContent(parser, false);
    }
}
