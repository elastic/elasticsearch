/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.application.rules;

import org.elasticsearch.client.internal.Client;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.search.SearchModule;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.threadpool.TestThreadPool;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.xcontent.ToXContent;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xcontent.XContentType;
import org.elasticsearch.xpack.application.search.SearchApplicationTestUtils;
import org.junit.Before;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static java.util.Collections.emptyList;
import static org.elasticsearch.common.xcontent.XContentHelper.toXContent;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertToXContentEquivalent;
import static org.elasticsearch.xpack.application.rules.QueryRuleCriteriaType.ALWAYS;
import static org.elasticsearch.xpack.application.rules.QueryRuleCriteriaType.CONTAINS;
import static org.elasticsearch.xpack.application.rules.QueryRuleCriteriaType.EXACT;
import static org.elasticsearch.xpack.application.rules.QueryRuleCriteriaType.FUZZY;
import static org.elasticsearch.xpack.application.rules.QueryRuleCriteriaType.GT;
import static org.elasticsearch.xpack.application.rules.QueryRuleCriteriaType.GTE;
import static org.elasticsearch.xpack.application.rules.QueryRuleCriteriaType.LT;
import static org.elasticsearch.xpack.application.rules.QueryRuleCriteriaType.LTE;
import static org.elasticsearch.xpack.application.rules.QueryRuleCriteriaType.PREFIX;
import static org.elasticsearch.xpack.application.rules.QueryRuleCriteriaType.SUFFIX;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class QueryRuleCriteriaTests extends ESTestCase {
    private NamedWriteableRegistry namedWriteableRegistry;
    private Client client;
    private ThreadPool threadPool;

    @Before
    public void registerNamedObjects() {
        SearchModule searchModule = new SearchModule(Settings.EMPTY, emptyList());

        List<NamedWriteableRegistry.Entry> namedWriteables = searchModule.getNamedWriteables();
        namedWriteableRegistry = new NamedWriteableRegistry(namedWriteables);
        client = mockClient();
    }

    private Client mockClient() {
        Client mockClient = mock(Client.class);
        threadPool = new TestThreadPool(getClass().getSimpleName());
        when(mockClient.threadPool()).thenReturn(threadPool);
        return mockClient;
    }

    @Override
    public void tearDown() throws Exception {
        threadPool.shutdown();
        super.tearDown();
    }

    public final void testRandomSerialization() throws IOException {
        for (int runs = 0; runs < 10; runs++) {
            QueryRuleCriteria testInstance = SearchApplicationTestUtils.randomQueryRuleCriteria();
            assertTransportSerialization(testInstance);
            assertXContent(testInstance, randomBoolean());
        }
    }

    public final void testAlwaysSerialization() throws IOException {
        for (int runs = 0; runs < 10; runs++) {
            QueryRuleCriteria testInstance = new QueryRuleCriteria(ALWAYS, null, null, null);
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

    public void testAlwaysToXContent() throws IOException {
        String content = XContentHelper.stripWhitespace("""
            {
              "type": "always"
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
        QueryRuleCriteriaType type = EXACT;
        QueryRuleCriteria queryRuleCriteria = new QueryRuleCriteria(type, "query", List.of("elastic"), null);
        assertTrue(queryRuleCriteria.isMatch(client, "elastic", type));
        assertFalse(queryRuleCriteria.isMatch(client, "elasticc", type));

        queryRuleCriteria = new QueryRuleCriteria(type, "zip_code", List.of("12345"), null);
        assertTrue(queryRuleCriteria.isMatch(client, 12345, type));
        assertTrue(queryRuleCriteria.isMatch(client, "12345", type));
        assertFalse(queryRuleCriteria.isMatch(client, "123456", type));
    }

    public void testFuzzyExactMatch() {
        QueryRuleCriteriaType type = FUZZY;
        QueryRuleCriteria queryRuleCriteria = new QueryRuleCriteria(type, "query", List.of("elastic"), null);
        assertTrue(queryRuleCriteria.isMatch(client, "elastic", type));
        assertTrue(queryRuleCriteria.isMatch(client, "elasticc", type));
        assertFalse(queryRuleCriteria.isMatch(client, "elastic elastic elastic elastic", type));
    }

    public void testPrefixMatch() {
        QueryRuleCriteriaType type = PREFIX;
        QueryRuleCriteria queryRuleCriteria = new QueryRuleCriteria(type, "query", List.of("elastic", "kibana"), null);
        assertTrue(queryRuleCriteria.isMatch(client, "elastic", type));
        assertTrue(queryRuleCriteria.isMatch(client, "kibana", type));
        assertTrue(queryRuleCriteria.isMatch(client, "elastic is a great search engine", type));
        assertTrue(queryRuleCriteria.isMatch(client, "kibana is a great visualization tool", type));
        assertFalse(queryRuleCriteria.isMatch(client, "you know, for search - elastic, kibana", type));
    }

    public void testSuffixMatch() {
        QueryRuleCriteriaType type = SUFFIX;
        QueryRuleCriteria queryRuleCriteria = new QueryRuleCriteria(type, "query", List.of("search", "lucene"), null);
        assertTrue(queryRuleCriteria.isMatch(client, "search", type));
        assertTrue(queryRuleCriteria.isMatch(client, "lucene", type));
        assertTrue(queryRuleCriteria.isMatch(client, "you know, for search", type));
        assertTrue(queryRuleCriteria.isMatch(client, "elasticsearch is built on top of lucene", type));
        assertFalse(queryRuleCriteria.isMatch(client, "search is a good use case for elastic", type));
        assertFalse(queryRuleCriteria.isMatch(client, "lucene and elastic are open source", type));
    }

    public void testContainsMatch() {
        QueryRuleCriteriaType type = CONTAINS;
        QueryRuleCriteria queryRuleCriteria = new QueryRuleCriteria(type, "query", List.of("elastic"), null);
        assertTrue(queryRuleCriteria.isMatch(client, "elastic", type));
        assertTrue(queryRuleCriteria.isMatch(client, "I use elastic for search", type));
        assertFalse(queryRuleCriteria.isMatch(client, "you know, for search", type));
    }

    public void testLtMatch() {
        QueryRuleCriteriaType type = LT;
        QueryRuleCriteria queryRuleCriteria = new QueryRuleCriteria(type, "age", List.of("10"), null);
        assertTrue(queryRuleCriteria.isMatch(client, 5, type));
        assertFalse(queryRuleCriteria.isMatch(client, 10, type));
        assertFalse(queryRuleCriteria.isMatch(client, 20, type));
    }

    public void testLteMatch() {
        QueryRuleCriteriaType type = LTE;
        QueryRuleCriteria queryRuleCriteria = new QueryRuleCriteria(type, "age", List.of("10"), null);
        assertTrue(queryRuleCriteria.isMatch(client, 5, type));
        assertTrue(queryRuleCriteria.isMatch(client, 10, type));
        assertFalse(queryRuleCriteria.isMatch(client, 20, type));
    }

    public void testGtMatch() {
        QueryRuleCriteriaType type = GT;
        QueryRuleCriteria queryRuleCriteria = new QueryRuleCriteria(type, "age", List.of("10"), null);
        assertTrue(queryRuleCriteria.isMatch(client, 20, type));
        assertFalse(queryRuleCriteria.isMatch(client, 10, type));
        assertFalse(queryRuleCriteria.isMatch(client, 5, type));
    }

    public void testGteMatch() {
        QueryRuleCriteriaType type = GTE;
        QueryRuleCriteria queryRuleCriteria = new QueryRuleCriteria(type, "age", List.of("10"), null);
        assertTrue(queryRuleCriteria.isMatch(client, 20, type));
        assertTrue(queryRuleCriteria.isMatch(client, 10, type));
        assertFalse(queryRuleCriteria.isMatch(client, 5, type));
    }

    public void testAlwaysMatch() {
        QueryRuleCriteriaType type = ALWAYS;
        QueryRuleCriteria queryRuleCriteria = new QueryRuleCriteria(type, null, null, null);
        assertTrue(queryRuleCriteria.isMatch(client, "elastic", type));
        assertTrue(queryRuleCriteria.isMatch(client, 42, type));
    }

    public void testInvalidCriteriaInput() {
        for (QueryRuleCriteriaType type : List.of(FUZZY, PREFIX, SUFFIX, CONTAINS)) {
            QueryRuleCriteria queryRuleCriteria = new QueryRuleCriteria(type, "foo", List.of("bar"), null);
            expectThrows(IllegalArgumentException.class, () -> queryRuleCriteria.isMatch(client, 42, type));
        }

        for (QueryRuleCriteriaType type : List.of(LT, LTE, GT, GTE)) {
            QueryRuleCriteria queryRuleCriteria = new QueryRuleCriteria(type, "foo", List.of(42), null);
            expectThrows(IllegalArgumentException.class, () -> queryRuleCriteria.isMatch(client, "puggles", type));
        }
    }

    public void testInvalidCriteriaProperties() {
        Map<String, Object> invalidProperties = Map.of("foo", "bar");
        for (QueryRuleCriteriaType type : QueryRuleCriteriaType.values()) {
            expectThrows(IllegalArgumentException.class, () -> new QueryRuleCriteria(type, "foo", List.of("bar"), invalidProperties));
        }

        Map<String, Object> inferProperties = QueryRuleCriteriaType.INFER.getAllowedPropertyNames()
            .stream()
            .map(name -> Map.entry(name, "foo"))
            .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
        inferProperties.put("inference_config", "text_expansion");
        new QueryRuleCriteria(QueryRuleCriteriaType.INFER, "foo", List.of("bar"), inferProperties);
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
