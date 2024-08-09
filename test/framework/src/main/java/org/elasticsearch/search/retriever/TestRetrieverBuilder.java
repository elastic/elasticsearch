/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.search.retriever;

import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.plugins.SearchPlugin;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xcontent.ConstructingObjectParser;
import org.elasticsearch.xcontent.ParseField;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentParser;

import java.io.IOException;
import java.util.Objects;

import static org.elasticsearch.xcontent.ConstructingObjectParser.constructorArg;

/**
 * Test retriever is used to test parsing of retrievers in plugins where
 * generation of other random retrievers are not easily accessible through test code.
 */
public class TestRetrieverBuilder extends RetrieverBuilder {

    /**
     * Creates a random {@link TestRetrieverBuilder}. The created instance
     * is not guaranteed to pass {@link SearchRequest} validation. This is purely
     * for x-content testing.
     */
    public static TestRetrieverBuilder createRandomTestRetrieverBuilder() {
        return new TestRetrieverBuilder(ESTestCase.randomAlphaOfLengthBetween(5, 10));
    }

    public static final String NAME = "test";
    public static final ParseField TEST_FIELD = new ParseField(NAME);
    public static final SearchPlugin.RetrieverSpec<TestRetrieverBuilder> TEST_SPEC = new SearchPlugin.RetrieverSpec<>(
        TEST_FIELD,
        TestRetrieverBuilder::fromXContent
    );

    public static final ParseField VALUE_FIELD = new ParseField("value");

    public static final ConstructingObjectParser<TestRetrieverBuilder, RetrieverParserContext> PARSER = new ConstructingObjectParser<>(
        NAME,
        args -> new TestRetrieverBuilder((String) args[0])
    );

    static {
        PARSER.declareString(constructorArg(), VALUE_FIELD);
    }

    public static TestRetrieverBuilder fromXContent(XContentParser parser, RetrieverParserContext context) {
        return PARSER.apply(parser, context);
    }

    private final String value;

    public TestRetrieverBuilder(String value) {
        this.value = value;
    }

    @Override
    public QueryBuilder topDocsQuery() {
        throw new UnsupportedOperationException("only used for parsing tests");
    }

    @Override
    public void extractToSearchSourceBuilder(SearchSourceBuilder searchSourceBuilder, boolean compoundUsed) {
        throw new UnsupportedOperationException("only used for parsing tests");
    }

    @Override
    public String getName() {
        return NAME;
    }

    @Override
    public void doToXContent(XContentBuilder builder, Params params) throws IOException {
        builder.field(VALUE_FIELD.getPreferredName(), value);
    }

    @Override
    public boolean doEquals(Object o) {
        TestRetrieverBuilder that = (TestRetrieverBuilder) o;
        return Objects.equals(value, that.value);
    }

    @Override
    public int doHashCode() {
        return Objects.hash(value);
    }
}
