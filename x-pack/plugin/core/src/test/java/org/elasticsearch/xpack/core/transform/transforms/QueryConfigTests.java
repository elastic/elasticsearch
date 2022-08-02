/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.core.transform.transforms;

import org.elasticsearch.common.ParsingException;
import org.elasticsearch.common.ValidationException;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.io.stream.Writeable.Reader;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.index.query.MatchAllQueryBuilder;
import org.elasticsearch.index.query.MatchNoneQueryBuilder;
import org.elasticsearch.index.query.MatchQueryBuilder;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.xcontent.ToXContent;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentFactory;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xcontent.XContentType;
import org.elasticsearch.xcontent.json.JsonXContent;
import org.elasticsearch.xpack.core.transform.AbstractSerializingTransformTestCase;
import org.elasticsearch.xpack.core.transform.MockDeprecatedQueryBuilder;
import org.junit.Before;

import java.io.IOException;
import java.util.LinkedHashMap;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;

public class QueryConfigTests extends AbstractSerializingTransformTestCase<QueryConfig> {

    private boolean lenient;

    public static QueryConfig randomQueryConfig() {

        QueryBuilder queryBuilder = randomBoolean() ? new MatchAllQueryBuilder() : new MatchNoneQueryBuilder();
        LinkedHashMap<String, Object> source = null;

        try (XContentBuilder xContentBuilder = XContentFactory.jsonBuilder()) {
            XContentBuilder content = queryBuilder.toXContent(xContentBuilder, ToXContent.EMPTY_PARAMS);
            source = (LinkedHashMap<String, Object>) XContentHelper.convertToMap(BytesReference.bytes(content), true, XContentType.JSON)
                .v2();
        } catch (IOException e) {
            // should not happen
            fail("failed to create random query config");
        }

        return new QueryConfig(source, queryBuilder);
    }

    public static QueryConfig randomInvalidQueryConfig() {
        // create something broken but with a source
        LinkedHashMap<String, Object> source = new LinkedHashMap<>();
        for (String key : randomUnique(() -> randomAlphaOfLengthBetween(1, 20), randomIntBetween(1, 10))) {
            source.put(key, randomAlphaOfLengthBetween(1, 20));
        }

        return new QueryConfig(source, null);
    }

    @Before
    public void setRandomFeatures() {
        lenient = randomBoolean();
    }

    @Override
    protected QueryConfig doParseInstance(XContentParser parser) throws IOException {
        return QueryConfig.fromXContent(parser, lenient);
    }

    @Override
    protected QueryConfig createTestInstance() {
        return lenient ? randomBoolean() ? randomQueryConfig() : randomInvalidQueryConfig() : randomQueryConfig();
    }

    @Override
    protected Reader<QueryConfig> instanceReader() {
        return QueryConfig::new;
    }

    public void testValidQueryParsing() throws IOException {
        QueryBuilder query = new MatchQueryBuilder("key", "value");
        String source = query.toString();
        try (XContentParser parser = createParser(JsonXContent.jsonXContent, source)) {
            QueryConfig queryConfig = QueryConfig.fromXContent(parser, true);
            assertNotNull(queryConfig.getQuery());
            assertEquals(query, queryConfig.getQuery());
        }
    }

    public void testFailOnStrictPassOnLenient() throws IOException {
        String source = "{\"query_element_does_not_exist\" : {}}";

        // lenient, passes but reports invalid
        try (XContentParser parser = createParser(JsonXContent.jsonXContent, source)) {
            QueryConfig query = QueryConfig.fromXContent(parser, true);
            assertNull(query.getQuery());
        }

        // strict throws
        try (XContentParser parser = createParser(JsonXContent.jsonXContent, source)) {
            expectThrows(ParsingException.class, () -> QueryConfig.fromXContent(parser, false));
        }
    }

    public void testFailOnEmptyQuery() throws IOException {
        String source = "";

        // lenient, passes but reports invalid
        try (XContentParser parser = createParser(JsonXContent.jsonXContent, source)) {
            QueryConfig query = QueryConfig.fromXContent(parser, true);
            assertNull(query.getQuery());
        }

        // strict throws
        try (XContentParser parser = createParser(JsonXContent.jsonXContent, source)) {
            expectThrows(IllegalArgumentException.class, () -> QueryConfig.fromXContent(parser, false));
        }
    }

    public void testFailOnEmptyQueryClause() throws IOException {
        String source = "{}";

        // lenient, passes but reports invalid
        try (XContentParser parser = createParser(JsonXContent.jsonXContent, source)) {
            QueryConfig query = QueryConfig.fromXContent(parser, true);
            assertNull(query.getQuery());
            ValidationException validationException = query.validate(null);
            assertThat(validationException, is(notNullValue()));
            assertThat(validationException.getMessage(), containsString("source.query must not be null"));
        }

        // strict throws
        try (XContentParser parser = createParser(JsonXContent.jsonXContent, source)) {
            expectThrows(IllegalArgumentException.class, () -> QueryConfig.fromXContent(parser, false));
        }
    }

    public void testDeprecation() throws IOException {
        String source = "{\"" + MockDeprecatedQueryBuilder.NAME + "\" : {}}";
        try (XContentParser parser = createParser(JsonXContent.jsonXContent, source)) {
            QueryConfig query = QueryConfig.fromXContent(parser, false);
            assertNotNull(query.getQuery());
            assertWarnings(MockDeprecatedQueryBuilder.DEPRECATION_MESSAGE);
        }
    }
}
