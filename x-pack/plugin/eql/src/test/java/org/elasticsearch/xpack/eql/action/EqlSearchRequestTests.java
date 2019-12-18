/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.eql.action;

import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.NamedXContentRegistry;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.common.xcontent.json.JsonXContent;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.search.SearchModule;
import org.elasticsearch.test.AbstractSerializingTestCase;
import org.junit.Before;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import static org.elasticsearch.index.query.AbstractQueryBuilder.parseInnerQueryBuilder;

public class EqlSearchRequestTests extends AbstractSerializingTestCase<EqlSearchRequest> {

    // TODO: possibly add mutations
    static String defaultTestQuery = "{\n" +
        "   \"match\" : {\n" +
        "       \"foo\": \"bar\"\n" +
        "   }" +
        "}";

    static String defaultTestIndex = "endgame-*";

    @Before
    public void setup() {
    }

    @Override
    protected NamedWriteableRegistry getNamedWriteableRegistry() {
        SearchModule searchModule = new SearchModule(Settings.EMPTY, Collections.emptyList());
        return new NamedWriteableRegistry(searchModule.getNamedWriteables());
    }

    @Override
    protected NamedXContentRegistry xContentRegistry() {
        SearchModule searchModule = new SearchModule(Settings.EMPTY, Collections.emptyList());
        return new NamedXContentRegistry(searchModule.getNamedXContents());
    }

    @Override
    protected EqlSearchRequest createTestInstance() {
        QueryBuilder query = null;
        try {
            query = parseQuery(defaultTestQuery);
        } catch (IOException ex) {
            assertNotNull("unexpected IOException " + ex.getCause().getMessage(), ex);
        }
        return new EqlSearchRequest(defaultTestIndex, query,
                randomAlphaOfLength(10), randomAlphaOfLength(10), randomAlphaOfLength(10),
                randomIntBetween(1, 50), randomSearchAfter(), randomAlphaOfLength(10));
    }

    protected QueryBuilder parseQuery(String queryAsString) throws IOException {
        XContentParser parser = createParser(JsonXContent.jsonXContent, queryAsString);
        return parseQuery(parser);
    }

    protected QueryBuilder parseQuery(XContentParser parser) throws IOException {
        QueryBuilder parseInnerQueryBuilder = parseInnerQueryBuilder(parser);
        assertNull(parser.nextToken());
        return parseInnerQueryBuilder;
    }

    private List<String> randomSearchAfter() {
        if (randomBoolean()) {
            return Collections.emptyList();
        } else {
            int size = randomIntBetween(1, 50);
            List<String> arr = new ArrayList<>(size);
            for (int i = 0; i < size; i++) {
                arr.add(randomAlphaOfLength(randomIntBetween(1, 15)));
            }
            return Collections.unmodifiableList(arr);
        }
    }

    @Override
    protected Writeable.Reader<EqlSearchRequest> instanceReader() {
        return EqlSearchRequest::new;
    }

    @Override
    protected EqlSearchRequest doParseInstance(XContentParser parser) {
        return EqlSearchRequest.fromXContent(parser).index(defaultTestIndex);
    }

    public void testSizeException() {
        final EqlSearchRequest eqlSearchRequest = createTestInstance();
        IllegalArgumentException e = expectThrows(IllegalArgumentException.class, () -> eqlSearchRequest.fetchSize(-1));
        assertEquals("size must be more than 0.", e.getMessage());

        e = expectThrows(IllegalArgumentException.class, () -> eqlSearchRequest.fetchSize(0));
        assertEquals("size must be more than 0.", e.getMessage());
    }
}
