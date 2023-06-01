/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.application.rules;

import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
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

public class QueryRulesetTests extends ESTestCase {
    private NamedWriteableRegistry namedWriteableRegistry;

    @Before
    public void registerNamedObjects() {
        SearchModule searchModule = new SearchModule(Settings.EMPTY, emptyList());

        List<NamedWriteableRegistry.Entry> namedWriteables = searchModule.getNamedWriteables();
        namedWriteableRegistry = new NamedWriteableRegistry(namedWriteables);
    }

    public final void testRandomSerialization() throws IOException {
        for (int runs = 0; runs < 10; runs++) {
            QueryRuleset testInstance = SearchApplicationTestUtils.randomQueryRuleset();
            assertTransportSerialization(testInstance);
            assertXContent(testInstance, randomBoolean());
//            assertIndexSerialization(testInstance); // TODO enable
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

    private void assertXContent(QueryRuleset queryRuleset, boolean humanReadable) throws IOException {
        BytesReference originalBytes = toShuffledXContent(queryRuleset, XContentType.JSON, ToXContent.EMPTY_PARAMS, humanReadable);
        QueryRule parsed;
        try (XContentParser parser = createParser(XContentType.JSON.xContent(), originalBytes)) {
            parsed = QueryRule.fromXContent(queryRuleset.id(), parser);
        }
        assertToXContentEquivalent(originalBytes, toXContent(parsed, XContentType.JSON, humanReadable), XContentType.JSON);
    }

    private void assertTransportSerialization(QueryRuleset testInstance) throws IOException {
        QueryRuleset deserializedInstance = copyInstance(testInstance);
        assertNotSame(testInstance, deserializedInstance);
        assertThat(testInstance, equalTo(deserializedInstance));
    }

    // TODO implement
//    private void assertIndexSerialization(QueryRuleset testInstance) throws IOException {
//        final QueryRuleset deserializedInstance;
//        try (BytesStreamOutput output = new BytesStreamOutput()) {
//            QueryRulesIndexService.writeQueryRulesetBinaryWithVersion(
//                testInstance,
//                output,
//                TransportVersion.MINIMUM_COMPATIBLE
//            );
//            try (
//                StreamInput in = new NamedWriteableAwareStreamInput(
//                    new InputStreamStreamInput(output.bytes().streamInput()),
//                    namedWriteableRegistry
//                )
//            ) {
//                deserializedInstance = QueryRulesIndexService.parseQueryRulesetBinaryWithVersion(in);
//            }
//        }
//        assertNotSame(testInstance, deserializedInstance);
//        assertThat(testInstance, equalTo(deserializedInstance));
//    }

    private QueryRuleset copyInstance(QueryRuleset instance) throws IOException {
        return copyWriteable(instance, namedWriteableRegistry, QueryRuleset::new);
    }
}
