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
        }
    }

    public void testToXContent() throws IOException {
        String content = XContentHelper.stripWhitespace("""
            {
              "rule_id": "my_query_rule",
              "type": "pinned",
              "criteria": [
                { "type": "exact", "metadata": "query_string", "value": "foo" }
              ],
              "actions": {
                "ids": ["id1", "id2"]
              }
            }""");

        QueryRule queryRule = QueryRule.fromXContentBytes(new BytesArray(content), XContentType.JSON);
        boolean humanReadable = true;
        BytesReference originalBytes = toShuffledXContent(queryRule, XContentType.JSON, ToXContent.EMPTY_PARAMS, humanReadable);
        QueryRule parsed;
        try (XContentParser parser = createParser(XContentType.JSON.xContent(), originalBytes)) {
            parsed = QueryRule.fromXContent(parser);
        }
        assertToXContentEquivalent(originalBytes, toXContent(parsed, XContentType.JSON, humanReadable), XContentType.JSON);
    }

    public void testToXContentMissingQueryRuleId() throws IOException {
        String content = XContentHelper.stripWhitespace("""
            {
              "type": "pinned",
              "criteria": [
                { "type": "exact", "metadata": "query_string", "value": "foo" }
              ],
              "actions": {
                  "ids": ["id1", "id2"]
                }
            }""");
        expectThrows(IllegalArgumentException.class, () -> QueryRule.fromXContentBytes(new BytesArray(content), XContentType.JSON));
    }

    public void testToXContentEmptyCriteria() throws IOException {
        String content = XContentHelper.stripWhitespace("""
            {
              "rule_id": "my_query_rule",
              "type": "pinned",
              "criteria": [],
              "actions": {}
            }""");
        expectThrows(IllegalArgumentException.class, () -> QueryRule.fromXContentBytes(new BytesArray(content), XContentType.JSON));
    }

    public void testToXContentValidPinnedRulesWithIds() throws IOException {
        String content = XContentHelper.stripWhitespace("""
            {
              "rule_id": "my_query_rule",
              "type": "pinned",
              "criteria": [
                { "type": "exact", "metadata": "query_string", "value": "foo" }
              ],
              "actions": {
                "ids": ["id1", "id2"]
              }
            }""");
        testToXContentPinnedRules(content);
    }

    public void testToXContentValidPinnedRulesWithDocs() throws IOException {
        String content = XContentHelper.stripWhitespace("""
            {
              "rule_id": "my_query_rule",
              "type": "pinned",
              "criteria": [
                { "type": "exact", "metadata": "query_string", "value": "foo" }
              ],
              "actions": {
                "docs": [
                  {
                    "_index": "foo",
                    "_id": "id1"
                  },
                  {
                    "_index": "bar",
                    "_id": "id2"
                  }
                ]
              }
            }""");
        testToXContentPinnedRules(content);
    }

    private void testToXContentPinnedRules(String content) throws IOException {
        QueryRule queryRule = QueryRule.fromXContentBytes(new BytesArray(content), XContentType.JSON);
        boolean humanReadable = true;
        BytesReference originalBytes = toShuffledXContent(queryRule, XContentType.JSON, ToXContent.EMPTY_PARAMS, humanReadable);
        QueryRule parsed;
        try (XContentParser parser = createParser(XContentType.JSON.xContent(), originalBytes)) {
            parsed = QueryRule.fromXContent(parser);
        }
        assertToXContentEquivalent(originalBytes, toXContent(parsed, XContentType.JSON, humanReadable), XContentType.JSON);
    }

    public void testToXContentPinnedRuleWithInvalidActions() throws IOException {
        String content = XContentHelper.stripWhitespace("""
            {
              "rule_id": "my_query_rule",
              "type": "pinned",
              "criteria": [
                { "type": "exact", "metadata": "query_string", "value": "foo" }
              ],
              "actions": {
                  "foo": "bar"
                }
            }""");
        expectThrows(IllegalArgumentException.class, () -> QueryRule.fromXContentBytes(new BytesArray(content), XContentType.JSON));
    }

    private void assertXContent(QueryRule queryRule, boolean humanReadable) throws IOException {
        BytesReference originalBytes = toShuffledXContent(queryRule, XContentType.JSON, ToXContent.EMPTY_PARAMS, humanReadable);
        QueryRule parsed;
        try (XContentParser parser = createParser(XContentType.JSON.xContent(), originalBytes)) {
            parsed = QueryRule.fromXContent(parser);
        }
        assertToXContentEquivalent(originalBytes, toXContent(parsed, XContentType.JSON, humanReadable), XContentType.JSON);
    }

    private void assertTransportSerialization(QueryRule testInstance) throws IOException {
        QueryRule deserializedInstance = copyInstance(testInstance);
        assertNotSame(testInstance, deserializedInstance);
        assertThat(testInstance, equalTo(deserializedInstance));
    }

    private QueryRule copyInstance(QueryRule instance) throws IOException {
        return copyWriteable(instance, namedWriteableRegistry, QueryRule::new);
    }
}
