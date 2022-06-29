/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.core.transform.transforms.latest;

import org.elasticsearch.common.io.stream.Writeable.Reader;
import org.elasticsearch.search.sort.SortBuilders;
import org.elasticsearch.search.sort.SortOrder;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xcontent.XContentParserConfiguration;
import org.elasticsearch.xcontent.XContentType;
import org.elasticsearch.xpack.core.transform.AbstractSerializingTransformTestCase;

import java.io.IOException;
import java.util.ArrayList;

import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.nullValue;

public class LatestConfigTests extends AbstractSerializingTransformTestCase<LatestConfig> {

    public static LatestConfig randomLatestConfig() {
        return new LatestConfig(
            new ArrayList<>(randomUnique(() -> randomAlphaOfLengthBetween(1, 10), randomIntBetween(1, 10))),
            randomAlphaOfLengthBetween(1, 10)
        );
    }

    @Override
    protected LatestConfig doParseInstance(XContentParser parser) throws IOException {
        return LatestConfig.fromXContent(parser, false);
    }

    @Override
    protected LatestConfig createTestInstance() {
        return randomLatestConfig();
    }

    @Override
    protected Reader<LatestConfig> instanceReader() {
        return LatestConfig::new;
    }

    public void testValidate_ValidConfig() throws IOException {
        String json = """
            { "unique_key": [ "event1", "event2", "event3" ], "sort": "timestamp"}""";

        LatestConfig config = createLatestConfigFromString(json);
        assertThat(config.validate(null), is(nullValue()));
        assertThat(config.getUniqueKey(), contains("event1", "event2", "event3"));
        assertThat(config.getSort(), is(equalTo("timestamp")));
        assertThat(config.getSorts(), contains(SortBuilders.fieldSort("timestamp").order(SortOrder.DESC)));
    }

    public void testValidate_EmptyUniqueKey() throws IOException {
        String json = """
            { "unique_key": [], "sort": "timestamp"}""";

        LatestConfig config = createLatestConfigFromString(json);
        assertThat(config.validate(null).validationErrors(), contains("latest.unique_key must be non-empty"));
    }

    public void testValidate_EmptyUniqueKeyElement() throws IOException {
        String json = """
            { "unique_key": [ "event1", "", "event2", "", "event3" ], "sort": "timestamp"}""";

        LatestConfig config = createLatestConfigFromString(json);
        assertThat(
            config.validate(null).validationErrors(),
            containsInAnyOrder("latest.unique_key[1] element must be non-empty", "latest.unique_key[3] element must be non-empty")
        );
    }

    public void testValidate_DuplicateUniqueKeyElement() throws IOException {
        String json = """
            { "unique_key": [ "event1", "event2", "event1" ], "sort": "timestamp"}""";

        LatestConfig config = createLatestConfigFromString(json);
        assertThat(
            config.validate(null).validationErrors(),
            containsInAnyOrder("latest.unique_key elements must be unique, found duplicate element [event1]")
        );
    }

    public void testValidate_EmptySort() throws IOException {
        String json = """
            { "unique_key": [ "event1", "event2", "event3" ], "sort": ""}""";

        LatestConfig config = createLatestConfigFromString(json);
        assertThat(config.validate(null).validationErrors(), contains("latest.sort must be non-empty"));
    }

    public void testValidate_EmptyUniqueKeyAndSort() throws IOException {
        String json = """
            { "unique_key": [], "sort": ""}""";

        LatestConfig config = createLatestConfigFromString(json);
        assertThat(
            config.validate(null).validationErrors(),
            containsInAnyOrder("latest.unique_key must be non-empty", "latest.sort must be non-empty")
        );
    }

    private LatestConfig createLatestConfigFromString(String json) throws IOException {
        final XContentParser parser = XContentType.JSON.xContent()
            .createParser(XContentParserConfiguration.EMPTY.withRegistry(xContentRegistry()), json);
        return LatestConfig.fromXContent(parser, false);
    }
}
