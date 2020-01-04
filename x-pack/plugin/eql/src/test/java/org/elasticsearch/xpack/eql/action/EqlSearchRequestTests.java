/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.eql.action;

import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.text.Text;
import org.elasticsearch.common.xcontent.NamedXContentRegistry;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.common.xcontent.json.JsonXContent;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.search.SearchModule;
import org.elasticsearch.search.searchafter.SearchAfterBuilder;
import org.elasticsearch.test.AbstractSerializingTestCase;
import org.junit.Before;

import java.io.IOException;
import java.util.Collections;

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
        try {
            QueryBuilder query = parseQuery(defaultTestQuery);
            return new EqlSearchRequest()
                .indices(new String[]{defaultTestIndex})
                .query(query)
                .timestampField(randomAlphaOfLength(10))
                .eventTypeField(randomAlphaOfLength(10))
                .implicitJoinKeyField(randomAlphaOfLength(10))
                .fetchSize(randomIntBetween(1, 50))
                .searchAfter(randomJsonSearchFromBuilder().getSortValues())
                .rule(randomAlphaOfLength(10));
        } catch (IOException ex) {
            assertNotNull("unexpected IOException " + ex.getCause().getMessage(), ex);
        }
        return null;
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

    private SearchAfterBuilder randomJsonSearchFromBuilder() throws IOException {
        int numSearchAfter = randomIntBetween(1, 10);
        XContentBuilder jsonBuilder = XContentFactory.jsonBuilder();
        jsonBuilder.startObject();
        jsonBuilder.startArray("search_after");
        for (int i = 0; i < numSearchAfter; i++) {
            int branch = randomInt(9);
            switch (branch) {
                case 0:
                    jsonBuilder.value(randomInt());
                    break;
                case 1:
                    jsonBuilder.value(randomFloat());
                    break;
                case 2:
                    jsonBuilder.value(randomLong());
                    break;
                case 3:
                    jsonBuilder.value(randomDouble());
                    break;
                case 4:
                    jsonBuilder.value(randomAlphaOfLengthBetween(5, 20));
                    break;
                case 5:
                    jsonBuilder.value(randomBoolean());
                    break;
                case 6:
                    jsonBuilder.value(randomByte());
                    break;
                case 7:
                    jsonBuilder.value(randomShort());
                    break;
                case 8:
                    jsonBuilder.value(new Text(randomAlphaOfLengthBetween(5, 20)));
                    break;
                case 9:
                    jsonBuilder.nullValue();
                    break;
            }
        }
        jsonBuilder.endArray();
        jsonBuilder.endObject();
        try (XContentParser parser = createParser(JsonXContent.jsonXContent, BytesReference.bytes(jsonBuilder))) {
            parser.nextToken();
            parser.nextToken();
            parser.nextToken();
            return SearchAfterBuilder.fromXContent(parser);
        }
    }

    @Override
    protected Writeable.Reader<EqlSearchRequest> instanceReader() {
        return EqlSearchRequest::new;
    }

    @Override
    protected EqlSearchRequest doParseInstance(XContentParser parser) {
        return EqlSearchRequest.fromXContent(parser).indices(new String[]{defaultTestIndex});
    }

    public void testSizeException() {
        final EqlSearchRequest eqlSearchRequest = createTestInstance();
        IllegalArgumentException e = expectThrows(IllegalArgumentException.class, () -> eqlSearchRequest.fetchSize(-1));
        assertEquals("size must be more than 0.", e.getMessage());

        e = expectThrows(IllegalArgumentException.class, () -> eqlSearchRequest.fetchSize(0));
        assertEquals("size must be more than 0.", e.getMessage());
    }
}
