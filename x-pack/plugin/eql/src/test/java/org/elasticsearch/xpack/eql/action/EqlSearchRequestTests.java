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
import org.elasticsearch.common.xcontent.DeprecationHandler;
import org.elasticsearch.common.xcontent.NamedXContentRegistry;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.common.xcontent.json.JsonXContent;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.search.SearchModule;
import org.elasticsearch.search.searchafter.SearchAfterBuilder;
import org.elasticsearch.test.AbstractSerializingTestCase;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.ql.expression.Order.OrderDirection;
import org.junit.Before;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.function.Supplier;

import static org.elasticsearch.index.query.AbstractQueryBuilder.parseInnerQueryBuilder;
import static org.elasticsearch.xpack.eql.action.RequestDefaults.FIELD_DEFAULT_ORDER;
import static org.elasticsearch.xpack.eql.action.RequestDefaults.FIELD_EVENT_CATEGORY;
import static org.elasticsearch.xpack.eql.action.RequestDefaults.FIELD_IMPLICIT_JOIN_KEY;
import static org.elasticsearch.xpack.eql.action.RequestDefaults.FIELD_TIMESTAMP;

public class EqlSearchRequestTests extends AbstractSerializingTestCase<EqlSearchRequest> {

    // TODO: possibly add mutations
    static String defaultTestFilter = "{\n" +
        "   \"match\" : {\n" +
        "       \"foo\": \"bar\"\n" +
        "   }" +
        "}";

    static String defaultTestIndex = "endgame-*";

    @Before
    public void setup() {
    }

    public void testRequestDefaults() throws IOException {
        String json = "{\"query\": \"process where true\"}";
        XContentParser parser = parser(json);
        EqlSearchRequest request = EqlSearchRequest.fromXContent(parser);
        
        assertEquals("process where true", request.query());
        assertNull(request.filter());
        assertEquals(FIELD_TIMESTAMP, request.timestampField());
        assertNull(request.tiebreakerField());
        assertEquals(FIELD_EVENT_CATEGORY, request.eventCategoryField());
        assertEquals(FIELD_IMPLICIT_JOIN_KEY, request.implicitJoinKeyField());
        assertEquals(RequestDefaults.SIZE, request.size());
        assertEquals(RequestDefaults.FETCH_SIZE, request.fetchSize());
        assertNull(request.searchAfter());
        assertEquals(false, request.isCaseSensitive());
        assertEquals(FIELD_DEFAULT_ORDER, request.defaultOrder());
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
            QueryBuilder filter = parseFilter(defaultTestFilter);
            EqlSearchRequest request = new EqlSearchRequest()
                .indices(new String[]{defaultTestIndex})
                .filter(filter)
                .timestampField(randomAlphaOfLength(10))
                .eventCategoryField(randomAlphaOfLength(10))
                .implicitJoinKeyField(randomAlphaOfLength(10))
                .fetchSize(randomIntBetween(1, 50))
                .isCaseSensitive(randomBoolean())
                .defaultOrder(randomFrom(OrderDirection.values()).toString())
                .query(randomAlphaOfLength(10));

            if (randomBoolean()) {
                request.searchAfter(randomJsonSearchFromBuilder());
            }
            return request;
        } catch (IOException ex) {
            assertNotNull("unexpected IOException " + ex.getCause().getMessage(), ex);
        }
        return null;
    }

    protected QueryBuilder parseFilter(String filter) throws IOException {
        XContentParser parser = createParser(JsonXContent.jsonXContent, filter);
        return parseFilter(parser);
    }

    protected QueryBuilder parseFilter(XContentParser parser) throws IOException {
        QueryBuilder parseInnerQueryBuilder = parseInnerQueryBuilder(parser);
        assertNull(parser.nextToken());
        return parseInnerQueryBuilder;
    }

    private Object randomValue() {
        Supplier<Object> value = randomFrom(Arrays.asList(
            ESTestCase::randomInt,
            ESTestCase::randomFloat,
            ESTestCase::randomLong,
            ESTestCase::randomDouble,
            () -> randomAlphaOfLengthBetween(5, 20),
            ESTestCase::randomBoolean,
            ESTestCase::randomByte,
            ESTestCase::randomShort,
            () -> new Text(randomAlphaOfLengthBetween(5, 20)),
            () -> null));
        return value.get();
    }

    private Object[] randomJsonSearchFromBuilder() throws IOException {
        int numSearchAfter = randomIntBetween(1, 10);
        XContentBuilder jsonBuilder = XContentFactory.jsonBuilder();
        jsonBuilder.startObject();
        jsonBuilder.startArray("search_after");
        for (int i = 0; i < numSearchAfter; i++) {
            jsonBuilder.value(randomValue());
        }
        jsonBuilder.endArray();
        jsonBuilder.endObject();
        try (XContentParser parser = createParser(JsonXContent.jsonXContent, BytesReference.bytes(jsonBuilder))) {
            parser.nextToken();
            parser.nextToken();
            parser.nextToken();
            return SearchAfterBuilder.fromXContent(parser).getSortValues();
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

    private XContentParser parser(String content) throws IOException {
        XContentType xContentType = XContentType.JSON;
        return xContentType.xContent().createParser(
                NamedXContentRegistry.EMPTY, DeprecationHandler.THROW_UNSUPPORTED_OPERATION, content);
    }
}
