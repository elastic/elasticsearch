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
import org.elasticsearch.xpack.application.EnterpriseSearchModuleTestUtils;
import org.elasticsearch.xpack.searchbusinessrules.SpecifiedDocument;
import org.junit.Before;

import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import static java.util.Collections.emptyList;
import static org.elasticsearch.common.xcontent.XContentHelper.toXContent;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertToXContentEquivalent;
import static org.elasticsearch.xpack.application.rules.QueryRuleCriteriaType.EXACT;
import static org.elasticsearch.xpack.application.rules.QueryRuleCriteriaType.PREFIX;
import static org.elasticsearch.xpack.application.rules.QueryRuleCriteriaType.SUFFIX;
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
            QueryRule testInstance = EnterpriseSearchModuleTestUtils.randomQueryRule();
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
                { "type": "exact", "metadata": "query_string", "values": ["foo", "bar"] }
              ],
              "actions": {
                "ids": ["id1", "id2"]
              },
              "priority": 5
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
                { "type": "exact", "metadata": "query_string", "values": ["foo", "bar"] }
              ],
              "actions": {
                "ids": ["id1", "id2"]
              }
            }""");
        testToXContentRules(content);
    }

    public void testToXContentValidExcludedRulesWithIds() throws IOException {
        String content = XContentHelper.stripWhitespace("""
            {
              "rule_id": "my_query_rule",
              "type": "exclude",
              "criteria": [
                { "type": "exact", "metadata": "query_string", "values": ["foo", "bar"] }
              ],
              "actions": {
                "ids": ["id1", "id2"]
              }
            }""");
        testToXContentRules(content);
    }

    public void testToXContentValidPinnedRulesWithDocs() throws IOException {
        String content = XContentHelper.stripWhitespace("""
            {
              "rule_id": "my_query_rule",
              "type": "pinned",
              "criteria": [
                { "type": "exact", "metadata": "query_string", "values": ["foo", "bar"] }
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
        testToXContentRules(content);
    }

    public void testToXContentValidExcludedRulesWithDocs() throws IOException {
        String content = XContentHelper.stripWhitespace("""
            {
              "rule_id": "my_query_rule",
              "type": "exclude",
              "criteria": [
                { "type": "exact", "metadata": "query_string", "values": ["foo", "bar"] }
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
        testToXContentRules(content);
    }

    public void testToXContentValidPinnedAndExcludedRulesWithIds() throws IOException {
        String content = XContentHelper.stripWhitespace("""
            {
              "rule_id": "my_pinned_query_rule",
              "type": "pinned",
              "criteria": [
                { "type": "exact", "metadata": "query_string", "values": ["foo", "bar"] }
              ],
              "actions": {
                "ids": ["id1", "id2"]
              }
            },
            {
              "rule_id": "my_exclude_query_rule",
              "type": "exlude",
              "criteria": [
                { "type": "exact", "metadata": "query_string", "values": ["baz"] }
              ],
              "actions": {
                "ids": ["id3", "id4"]
              }
            }""");
        testToXContentRules(content);
    }

    public void testToXContentValidPinnedAndExcludedRulesWithDocs() throws IOException {
        String content = XContentHelper.stripWhitespace("""
            {
              "rule_id": "my_pinned_query_rule",
              "type": "pinned",
              "criteria": [
                { "type": "exact", "metadata": "query_string", "values": ["foo", "bar"] }
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
            },
            {
              "rule_id": "my_exclude_query_rule",
              "type": "exclude",
              "criteria": [
                { "type": "exact", "metadata": "query_string", "values": ["foo", "bar"] }
              ],
              "actions": {
                "docs": [
                  {
                    "_index": "foo",
                    "_id": "id3"
                  },
                  {
                    "_index": "bar",
                    "_id": "id4"
                  }
                ]
              }
            }""");
        testToXContentRules(content);
    }

    private void testToXContentRules(String content) throws IOException {
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
                { "type": "exact", "metadata": "query_string", "values": ["foo", "bar"] }
              ],
              "actions": {
                  "foo": "bar"
                }
            }""");
        expectThrows(IllegalArgumentException.class, () -> QueryRule.fromXContentBytes(new BytesArray(content), XContentType.JSON));
    }

    public void testToXContentExcludeRuleWithInvalidActions() throws IOException {
        String content = XContentHelper.stripWhitespace("""
            {
              "rule_id": "my_query_rule",
              "type": "exclude",
              "criteria": [
                { "type": "exact", "metadata": "query_string", "values": ["foo", "bar"] }
              ],
              "actions": {
                  "foo": "bar"
                }
            }""");
        expectThrows(IllegalArgumentException.class, () -> QueryRule.fromXContentBytes(new BytesArray(content), XContentType.JSON));
    }

    public void testApplyPinnedRuleWithOneCriteria() {
        QueryRule rule = new QueryRule(
            randomAlphaOfLength(10),
            QueryRule.QueryRuleType.PINNED,
            List.of(new QueryRuleCriteria(EXACT, "query", List.of("elastic"))),
            Map.of("ids", List.of("id1", "id2")),
            EnterpriseSearchModuleTestUtils.randomQueryRulePriority()
        );
        AppliedQueryRules appliedQueryRules = new AppliedQueryRules();
        rule.applyRule(appliedQueryRules, Map.of("query", "elastic"));
        assertEquals(List.of(new SpecifiedDocument(null, "id1"), new SpecifiedDocument(null, "id2")), appliedQueryRules.pinnedDocs());
        assertEquals(Collections.emptyList(), appliedQueryRules.excludedDocs());

        appliedQueryRules = new AppliedQueryRules();
        rule.applyRule(appliedQueryRules, Map.of("query", "elastic1"));
        assertEquals(Collections.emptyList(), appliedQueryRules.pinnedDocs());
        assertEquals(Collections.emptyList(), appliedQueryRules.excludedDocs());
    }

    public void testApplyExcludeRuleWithOneCriteria() {
        QueryRule rule = new QueryRule(
            randomAlphaOfLength(10),
            QueryRule.QueryRuleType.EXCLUDE,
            List.of(new QueryRuleCriteria(EXACT, "query", List.of("elastic"))),
            Map.of("ids", List.of("id1", "id2")),
            EnterpriseSearchModuleTestUtils.randomQueryRulePriority()
        );
        AppliedQueryRules appliedQueryRules = new AppliedQueryRules();
        rule.applyRule(appliedQueryRules, Map.of("query", "elastic"));
        assertEquals(List.of(new SpecifiedDocument(null, "id1"), new SpecifiedDocument(null, "id2")), appliedQueryRules.excludedDocs());
        assertEquals(Collections.emptyList(), appliedQueryRules.pinnedDocs());

        appliedQueryRules = new AppliedQueryRules();
        rule.applyRule(appliedQueryRules, Map.of("query", "elastic1"));
        assertEquals(Collections.emptyList(), appliedQueryRules.excludedDocs());
        assertEquals(Collections.emptyList(), appliedQueryRules.pinnedDocs());
    }

    public void testApplyRuleWithMultipleCriteria() throws IOException {
        QueryRule rule = new QueryRule(
            randomAlphaOfLength(10),
            QueryRule.QueryRuleType.PINNED,
            List.of(new QueryRuleCriteria(PREFIX, "query", List.of("elastic")), new QueryRuleCriteria(SUFFIX, "query", List.of("search"))),
            Map.of("ids", List.of("id1", "id2")),
            EnterpriseSearchModuleTestUtils.randomQueryRulePriority()
        );
        AppliedQueryRules appliedQueryRules = new AppliedQueryRules();
        rule.applyRule(appliedQueryRules, Map.of("query", "elastic - you know, for search"));
        assertEquals(List.of(new SpecifiedDocument(null, "id1"), new SpecifiedDocument(null, "id2")), appliedQueryRules.pinnedDocs());
        assertEquals(Collections.emptyList(), appliedQueryRules.excludedDocs());
        assertEquals(Collections.emptyList(), appliedQueryRules.excludedDocs());

        appliedQueryRules = new AppliedQueryRules();
        rule.applyRule(appliedQueryRules, Map.of("query", "elastic"));
        assertEquals(Collections.emptyList(), appliedQueryRules.pinnedDocs());
        assertEquals(Collections.emptyList(), appliedQueryRules.excludedDocs());
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
