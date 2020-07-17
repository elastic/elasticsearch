/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.eql.action;

import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.text.Text;
import org.elasticsearch.common.xcontent.NamedXContentRegistry;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.common.xcontent.json.JsonXContent;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.search.SearchModule;
import org.elasticsearch.test.AbstractSerializingTestCase;
import org.elasticsearch.test.ESTestCase;
import org.junit.Before;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.function.Supplier;

import static org.elasticsearch.index.query.AbstractQueryBuilder.parseInnerQueryBuilder;

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
                .fetchSize(randomIntBetween(1, 50))
                .query(randomAlphaOfLength(10));

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

    @Override
    protected Writeable.Reader<EqlSearchRequest> instanceReader() {
        return EqlSearchRequest::new;
    }

    @Override
    protected EqlSearchRequest doParseInstance(XContentParser parser) {
        return EqlSearchRequest.fromXContent(parser).indices(new String[]{defaultTestIndex});
    }
}
