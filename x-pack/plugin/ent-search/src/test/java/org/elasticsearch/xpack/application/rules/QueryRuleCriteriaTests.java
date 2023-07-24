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
import static org.elasticsearch.xpack.application.rules.QueryRuleCriteria.CriteriaType.CONTAINS;
import static org.elasticsearch.xpack.application.rules.QueryRuleCriteria.CriteriaType.EXACT_FUZZY;
import static org.elasticsearch.xpack.application.rules.QueryRuleCriteria.CriteriaType.GLOBAL;
import static org.elasticsearch.xpack.application.rules.QueryRuleCriteria.CriteriaType.GT;
import static org.elasticsearch.xpack.application.rules.QueryRuleCriteria.CriteriaType.GTE;
import static org.elasticsearch.xpack.application.rules.QueryRuleCriteria.CriteriaType.LT;
import static org.elasticsearch.xpack.application.rules.QueryRuleCriteria.CriteriaType.LTE;
import static org.elasticsearch.xpack.application.rules.QueryRuleCriteria.CriteriaType.PREFIX;
import static org.elasticsearch.xpack.application.rules.QueryRuleCriteria.CriteriaType.SUFFIX;
import static org.hamcrest.CoreMatchers.equalTo;

public class QueryRuleCriteriaTests extends ESTestCase {
    private NamedWriteableRegistry namedWriteableRegistry;

    @Before
    public void registerNamedObjects() {
        SearchModule searchModule = new SearchModule(Settings.EMPTY, emptyList());

        List<NamedWriteableRegistry.Entry> namedWriteables = searchModule.getNamedWriteables();
        namedWriteableRegistry = new NamedWriteableRegistry(namedWriteables);
    }

    public final void testRandomSerialization() throws IOException {
        for (int runs = 0; runs < 10; runs++) {
            QueryRuleCriteria testInstance = SearchApplicationTestUtils.randomQueryRuleCriteria();
            assertTransportSerialization(testInstance);
            assertXContent(testInstance, randomBoolean());
        }
    }

    public final void testGlobalSerialization() throws IOException {
        for (int runs = 0; runs < 10; runs++) {
            QueryRuleCriteria testInstance = SearchApplicationTestUtils.randomGlobalQueryRuleCriteria();
            assertTransportSerialization(testInstance);
            assertXContent(testInstance, randomBoolean());
        }
    }

    public void testToXContent() throws IOException {
        String content = XContentHelper.stripWhitespace("""
            {
              "type": "exact",
              "metadata": "my-key",
              "values": ["foo","bar"]
            }""");

        QueryRuleCriteria queryRuleCriteria = QueryRuleCriteria.fromXContentBytes(new BytesArray(content), XContentType.JSON);
        boolean humanReadable = true;
        BytesReference originalBytes = toShuffledXContent(queryRuleCriteria, XContentType.JSON, ToXContent.EMPTY_PARAMS, humanReadable);
        QueryRuleCriteria parsed;
        try (XContentParser parser = createParser(XContentType.JSON.xContent(), originalBytes)) {
            parsed = QueryRuleCriteria.fromXContent(parser);
        }
        assertToXContentEquivalent(originalBytes, toXContent(parsed, XContentType.JSON, humanReadable), XContentType.JSON);
    }

    public void testGlobalToXContent() throws IOException {
        String content = XContentHelper.stripWhitespace("""
            {
              "type": "global"
            }""");

        QueryRuleCriteria queryRuleCriteria = QueryRuleCriteria.fromXContentBytes(new BytesArray(content), XContentType.JSON);
        boolean humanReadable = true;
        BytesReference originalBytes = toShuffledXContent(queryRuleCriteria, XContentType.JSON, ToXContent.EMPTY_PARAMS, humanReadable);
        QueryRuleCriteria parsed;
        try (XContentParser parser = createParser(XContentType.JSON.xContent(), originalBytes)) {
            parsed = QueryRuleCriteria.fromXContent(parser);
        }
        assertToXContentEquivalent(originalBytes, toXContent(parsed, XContentType.JSON, humanReadable), XContentType.JSON);
    }

    public void testExactMatch() {
        QueryRuleCriteria.CriteriaType type = QueryRuleCriteria.CriteriaType.EXACT;
        QueryRuleCriteria queryRuleCriteria = new QueryRuleCriteria(type, "query", List.of("elastic"));
        assertTrue(queryRuleCriteria.isMatch("elastic", type));
        assertFalse(queryRuleCriteria.isMatch("elasticc", type));

        queryRuleCriteria = new QueryRuleCriteria(type, "zip_code", List.of("12345"));
        assertTrue(queryRuleCriteria.isMatch(12345, type));
        assertTrue(queryRuleCriteria.isMatch("12345", type));
        assertFalse(queryRuleCriteria.isMatch("123456", type));
    }

    public void testFuzzyExactMatch() {
        QueryRuleCriteria.CriteriaType type = EXACT_FUZZY;
        QueryRuleCriteria queryRuleCriteria = new QueryRuleCriteria(type, "query", List.of("elastic"));
        assertTrue(queryRuleCriteria.isMatch("elastic", type));
        assertTrue(queryRuleCriteria.isMatch("elasticc", type));
        assertFalse(queryRuleCriteria.isMatch("elastic elastic elastic elastic", type));
    }

    public void testPrefixMatch() {
        QueryRuleCriteria.CriteriaType type = PREFIX;
        QueryRuleCriteria queryRuleCriteria = new QueryRuleCriteria(type, "query", List.of("elastic", "kibana"));
        assertTrue(queryRuleCriteria.isMatch("elastic", type));
        assertTrue(queryRuleCriteria.isMatch("kibana", type));
        assertTrue(queryRuleCriteria.isMatch("elastic is a great search engine", type));
        assertTrue(queryRuleCriteria.isMatch("kibana is a great visualization tool", type));
        assertFalse(queryRuleCriteria.isMatch("you know, for search - elastic, kibana", type));
    }

    public void testSuffixMatch() {
        QueryRuleCriteria.CriteriaType type = SUFFIX;
        QueryRuleCriteria queryRuleCriteria = new QueryRuleCriteria(type, "query", List.of("search", "lucene"));
        assertTrue(queryRuleCriteria.isMatch("search", type));
        assertTrue(queryRuleCriteria.isMatch("lucene", type));
        assertTrue(queryRuleCriteria.isMatch("you know, for search", type));
        assertTrue(queryRuleCriteria.isMatch("elasticsearch is built on top of lucene", type));
        assertFalse(queryRuleCriteria.isMatch("search is a good use case for elastic", type));
        assertFalse(queryRuleCriteria.isMatch("lucene and elastic are open source", type));
    }

    public void testContainsMatch() {
        QueryRuleCriteria.CriteriaType type = CONTAINS;
        QueryRuleCriteria queryRuleCriteria = new QueryRuleCriteria(type, "query", List.of("elastic"));
        assertTrue(queryRuleCriteria.isMatch("elastic", type));
        assertTrue(queryRuleCriteria.isMatch("I use elastic for search", type));
        assertFalse(queryRuleCriteria.isMatch("you know, for search", type));
    }

    public void testLtMatch() {
        QueryRuleCriteria.CriteriaType type = LT;
        QueryRuleCriteria queryRuleCriteria = new QueryRuleCriteria(type, "age", List.of("10"));
        assertTrue(queryRuleCriteria.isMatch(5, type));
        assertFalse(queryRuleCriteria.isMatch(10, type));
        assertFalse(queryRuleCriteria.isMatch(20, type));
    }

    public void testLteMatch() {
        QueryRuleCriteria.CriteriaType type = LTE;
        QueryRuleCriteria queryRuleCriteria = new QueryRuleCriteria(type, "age", List.of("10"));
        assertTrue(queryRuleCriteria.isMatch(5, type));
        assertTrue(queryRuleCriteria.isMatch(10, type));
        assertFalse(queryRuleCriteria.isMatch(20, type));
    }

    public void testGtMatch() {
        QueryRuleCriteria.CriteriaType type = GT;
        QueryRuleCriteria queryRuleCriteria = new QueryRuleCriteria(type, "age", List.of("10"));
        assertTrue(queryRuleCriteria.isMatch(20, type));
        assertFalse(queryRuleCriteria.isMatch(10, type));
        assertFalse(queryRuleCriteria.isMatch(5, type));
    }

    public void testGteMatch() {
        QueryRuleCriteria.CriteriaType type = GTE;
        QueryRuleCriteria queryRuleCriteria = new QueryRuleCriteria(type, "age", List.of("10"));
        assertTrue(queryRuleCriteria.isMatch(20, type));
        assertTrue(queryRuleCriteria.isMatch(10, type));
        assertFalse(queryRuleCriteria.isMatch(5, type));
    }

    public void testGlobalMatch() {
        QueryRuleCriteria.CriteriaType type = GLOBAL;
        QueryRuleCriteria queryRuleCriteria = new QueryRuleCriteria(type, null, null);
        assertTrue(queryRuleCriteria.isMatch("elastic", type));
        assertTrue(queryRuleCriteria.isMatch(42, type));
    }

    public void testInvalidCriteriaInput() {
        for (QueryRuleCriteria.CriteriaType type : List.of(EXACT_FUZZY, PREFIX, SUFFIX, CONTAINS)) {
            QueryRuleCriteria queryRuleCriteria = new QueryRuleCriteria(type, "foo", List.of("bar"));
            expectThrows(IllegalArgumentException.class, () -> queryRuleCriteria.isMatch(42, type));
        }

        for (QueryRuleCriteria.CriteriaType type : List.of(LT, LTE, GT, GTE)) {
            QueryRuleCriteria queryRuleCriteria = new QueryRuleCriteria(type, "foo", List.of(42));
            expectThrows(IllegalArgumentException.class, () -> queryRuleCriteria.isMatch("puggles", type));
        }
    }

    private void assertXContent(QueryRuleCriteria queryRule, boolean humanReadable) throws IOException {
        BytesReference originalBytes = toShuffledXContent(queryRule, XContentType.JSON, ToXContent.EMPTY_PARAMS, humanReadable);
        QueryRuleCriteria parsed;
        try (XContentParser parser = createParser(XContentType.JSON.xContent(), originalBytes)) {
            parsed = QueryRuleCriteria.fromXContent(parser);
        }
        assertToXContentEquivalent(originalBytes, toXContent(parsed, XContentType.JSON, humanReadable), XContentType.JSON);
    }

    private void assertTransportSerialization(QueryRuleCriteria testInstance) throws IOException {
        QueryRuleCriteria deserializedInstance = copyInstance(testInstance);
        assertNotSame(testInstance, deserializedInstance);
        assertThat(testInstance, equalTo(deserializedInstance));
    }

    private QueryRuleCriteria copyInstance(QueryRuleCriteria instance) throws IOException {
        return copyWriteable(instance, namedWriteableRegistry, QueryRuleCriteria::new);
    }
}
