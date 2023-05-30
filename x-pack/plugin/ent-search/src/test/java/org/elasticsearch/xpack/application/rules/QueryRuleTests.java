/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.application.rules;

import org.elasticsearch.TransportVersion;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.elasticsearch.common.io.stream.InputStreamStreamInput;
import org.elasticsearch.common.io.stream.NamedWriteableAwareStreamInput;
import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.search.SearchModule;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xcontent.ToXContent;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xcontent.XContentType;
import org.elasticsearch.xpack.application.search.SearchApplicationTestUtils;
import org.junit.Before;

import java.io.IOException;
import java.util.List;

import static java.util.Collections.emptyList;
import static org.elasticsearch.common.xcontent.XContentHelper.toXContent;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertToXContentEquivalent;
import static org.hamcrest.CoreMatchers.equalTo;

public class QueryRuleTests extends ESTestCase {
    private NamedWriteableRegistry namedWriteableRegistry;

    @Before
    public void registerNamedObjects() {
        SearchModule searchModule = new SearchModule(Settings.EMPTY, emptyList());

        List<NamedWriteableRegistry.Entry> namedWriteables = searchModule.getNamedWriteables();
        namedWriteableRegistry = new NamedWriteableRegistry(namedWriteables);
    }

    public final void testRandomSerialization() throws IOException {
        for (int runs = 0; runs < 10; runs++) {
            QueryRule testInstance = SearchApplicationTestUtils.randomQueryRule();
            assertTransportSerialization(testInstance);
            assertXContent(testInstance, randomBoolean());
            assertIndexSerialization(testInstance);
        }
    }

    public void testToXContent() throws IOException {
        String content = XContentHelper.stripWhitespace("""
            {
              "id": "my_query_rule",
              "type": "pinned"
            }""");

        QueryRule queryRule = QueryRule.fromXContentBytes("my_query_rule", new BytesArray(content), XContentType.JSON);
        boolean humanReadable = true;
        BytesReference originalBytes = toShuffledXContent(queryRule, XContentType.JSON, ToXContent.EMPTY_PARAMS, humanReadable);
        QueryRule parsed;
        try (XContentParser parser = createParser(XContentType.JSON.xContent(), originalBytes)) {
            parsed = QueryRule.fromXContent(queryRule.id(), parser);
        }
        assertToXContentEquivalent(originalBytes, toXContent(parsed, XContentType.JSON, humanReadable), XContentType.JSON);
    }

    public void testToXContentInvalidQueryRuleId() throws IOException {
        String content = XContentHelper.stripWhitespace("""
            {
              "name": "not_my_query_rule",
              "type": "pinned"
            }""");
        expectThrows(
            IllegalArgumentException.class,
            () -> QueryRule.fromXContentBytes("my_query_rule", new BytesArray(content), XContentType.JSON)
        );
    }

    // TODO implement this once we have more content in query rules.
//    public void testMerge() throws IOException {
//        String content = """
//            {
//              "indices": ["my_index", "my_index_2"],
//              "updated_at_millis": 0
//            }""";
//
//        String update = """
//            {
//              "indices": ["my_index_2", "my_index"],
//              "analytics_collection_name": "my_search_app_analytics",
//              "updated_at_millis": 12345
//            }""";
//        SearchApplication app = SearchApplication.fromXContentBytes("my_search_app", new BytesArray(content), XContentType.JSON);
//        SearchApplication updatedApp = app.merge(new BytesArray(update), XContentType.JSON, BigArrays.NON_RECYCLING_INSTANCE);
//        assertNotSame(app, updatedApp);
//        assertThat(updatedApp.indices(), equalTo(new String[] { "my_index", "my_index_2" }));
//        assertThat(updatedApp.analyticsCollectionName(), equalTo("my_search_app_analytics"));
//        assertThat(updatedApp.updatedAtMillis(), equalTo(12345L));
//    }

    private void assertXContent(QueryRule queryRule, boolean humanReadable) throws IOException {
        BytesReference originalBytes = toShuffledXContent(queryRule, XContentType.JSON, ToXContent.EMPTY_PARAMS, humanReadable);
        QueryRule parsed;
        try (XContentParser parser = createParser(XContentType.JSON.xContent(), originalBytes)) {
            parsed = QueryRule.fromXContent(queryRule.id(), parser);
        }
        assertToXContentEquivalent(originalBytes, toXContent(parsed, XContentType.JSON, humanReadable), XContentType.JSON);
    }

    private void assertTransportSerialization(QueryRule testInstance) throws IOException {
        QueryRule deserializedInstance = copyInstance(testInstance);
        assertNotSame(testInstance, deserializedInstance);
        assertThat(testInstance, equalTo(deserializedInstance));
    }

    private void assertIndexSerialization(QueryRule testInstance) throws IOException {
        final QueryRule deserializedInstance;
        try (BytesStreamOutput output = new BytesStreamOutput()) {
            QueryRuleIndexService.writeQueryRuleBinaryWithVersion(
                testInstance,
                output,
                TransportVersion.MINIMUM_COMPATIBLE
            );
            try (
                StreamInput in = new NamedWriteableAwareStreamInput(
                    new InputStreamStreamInput(output.bytes().streamInput()),
                    namedWriteableRegistry
                )
            ) {
                deserializedInstance = QueryRuleIndexService.parseQueryRuleBinaryWithVersion(in);
            }
        }
        assertNotSame(testInstance, deserializedInstance);
        assertThat(testInstance, equalTo(deserializedInstance));
    }

    private QueryRule copyInstance(QueryRule instance) throws IOException {
        return copyWriteable(instance, namedWriteableRegistry, QueryRule::new);
    }
}
