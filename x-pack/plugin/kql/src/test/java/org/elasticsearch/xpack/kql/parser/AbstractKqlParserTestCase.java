/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.kql.parser;

import org.elasticsearch.common.Strings;
import org.elasticsearch.common.compress.CompressedXContent;
import org.elasticsearch.common.regex.Regex;
import org.elasticsearch.core.Predicates;
import org.elasticsearch.core.SuppressForbidden;
import org.elasticsearch.index.mapper.MapperService;
import org.elasticsearch.index.query.MatchPhraseQueryBuilder;
import org.elasticsearch.index.query.MatchQueryBuilder;
import org.elasticsearch.index.query.MultiMatchQueryBuilder;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryStringQueryBuilder;
import org.elasticsearch.index.query.RangeQueryBuilder;
import org.elasticsearch.index.query.TermQueryBuilder;
import org.elasticsearch.index.query.WildcardQueryBuilder;
import org.elasticsearch.test.AbstractBuilderTestCase;
import org.elasticsearch.xcontent.XContentBuilder;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.URL;
import java.net.URLConnection;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;
import java.util.function.Consumer;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.elasticsearch.xcontent.XContentFactory.jsonBuilder;
import static org.hamcrest.Matchers.anEmptyMap;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.equalTo;

public abstract class AbstractKqlParserTestCase extends AbstractBuilderTestCase {
    protected static final String SUPPORTED_QUERY_FILE_PATH = "/supported-queries";
    protected static final String UNSUPPORTED_QUERY_FILE_PATH = "/unsupported-queries";
    protected static final Predicate<String> BOOLEAN_QUERY_FILTER = (q) -> q.matches("(?i)[^{]*[^\\\\]*(NOT|AND|OR)[^}]*");
    protected static final String NESTED_FIELD_NAME = "mapped_nested";

    @Override
    protected void initializeAdditionalMappings(MapperService mapperService) throws IOException {
        XContentBuilder mapping = jsonBuilder().startObject().startObject("_doc").startObject("properties");

        mapping.startObject(TEXT_FIELD_NAME).field("type", "text").endObject();
        mapping.startObject(NESTED_FIELD_NAME);
        {
            mapping.field("type", "nested");
            mapping.startObject("properties");
            {
                mapping.startObject(TEXT_FIELD_NAME).field("type", "text").endObject();
                mapping.startObject(KEYWORD_FIELD_NAME).field("type", "keyword").endObject();
                mapping.startObject(INT_FIELD_NAME).field("type", "integer").endObject();
                mapping.startObject(NESTED_FIELD_NAME);
                {
                    mapping.field("type", "nested");
                    mapping.startObject("properties");
                    {
                        mapping.startObject(TEXT_FIELD_NAME).field("type", "text").endObject();
                        mapping.startObject(KEYWORD_FIELD_NAME).field("type", "keyword").endObject();
                        mapping.startObject(INT_FIELD_NAME).field("type", "integer").endObject();
                    }
                    mapping.endObject();
                }
                mapping.endObject();
            }
            mapping.endObject();
        }
        mapping.endObject();

        mapping.endObject().endObject().endObject();

        mapperService.merge("_doc", new CompressedXContent(Strings.toString(mapping)), MapperService.MergeReason.MAPPING_UPDATE);
    }

    protected static String wrapWithRandomWhitespaces(String input) {
        return String.join("", randomWhitespaces(), input, randomWhitespaces());
    }

    protected static String randomWhitespaces() {
        return randomWhitespaces(randomInt(20));
    }

    protected static String randomWhitespaces(int length) {
        return Stream.generate(() -> randomFrom(" ", "\t", "\n", "\r", "\u3000")).limit(length).collect(Collectors.joining());
    }

    protected static List<String> readQueries(String source) throws IOException {
        return readQueries(source, Predicates.always());
    }

    protected static List<String> readQueries(String source, Predicate<String> filter) throws IOException {
        URL url = KqlParserTests.class.getResource(source);
        Objects.requireNonNull(source, "Cannot find resource " + url);

        List<String> queries = new ArrayList<>();

        try (BufferedReader reader = new BufferedReader(new InputStreamReader(readFromJarUrl(url), StandardCharsets.UTF_8))) {
            String line;

            while ((line = reader.readLine()) != null) {
                String query = line.trim();
                // ignore comments
                if (query.isEmpty() == false && query.startsWith("//") == false && filter.test(query)) {
                    queries.add(query);
                }
            }
        }
        return queries;
    }

    @SuppressForbidden(reason = "test reads from jar")
    private static InputStream readFromJarUrl(URL source) throws IOException {
        URLConnection con = source.openConnection();
        // do not to cache files (to avoid keeping file handles around)
        con.setUseCaches(false);
        return con.getInputStream();
    }

    protected List<String> mappedLeafFields() {
        return Stream.concat(
            Arrays.stream(MAPPED_LEAF_FIELD_NAMES),
            Stream.of(
                // Adding mapped_object subfields
                Strings.format("%s.%s", OBJECT_FIELD_NAME, INT_FIELD_NAME),
                Strings.format("%s.%s", OBJECT_FIELD_NAME, DATE_FIELD_NAME),
                // Adding mapped_nested subfields
                Strings.format("%s.%s", NESTED_FIELD_NAME, TEXT_FIELD_NAME),
                Strings.format("%s.%s", NESTED_FIELD_NAME, KEYWORD_FIELD_NAME),
                Strings.format("%s.%s", NESTED_FIELD_NAME, INT_FIELD_NAME),
                Strings.format("%s.%s.%s", NESTED_FIELD_NAME, NESTED_FIELD_NAME, TEXT_FIELD_NAME),
                Strings.format("%s.%s.%s", NESTED_FIELD_NAME, NESTED_FIELD_NAME, KEYWORD_FIELD_NAME),
                Strings.format("%s.%s.%s", NESTED_FIELD_NAME, NESTED_FIELD_NAME, INT_FIELD_NAME)
            )
        ).toList();
    }

    protected List<String> searchableFields() {
        return Stream.concat(
            mappedLeafFields().stream().filter(fieldName -> fieldName.equals(BINARY_FIELD_NAME) == false),
            Stream.of("_id", "_index")
        ).toList();
    }

    protected List<String> searchableFields(String fieldNamePattern) {
        return searchableFields().stream().filter(fieldName -> Regex.simpleMatch(fieldNamePattern, fieldName)).toList();
    }

    protected QueryBuilder parseKqlQuery(String kqlQuery) {
        KqlParser parser = new KqlParser();
        KqlParsingContext kqlParserContext = KqlParsingContext.builder(createQueryRewriteContext()).build();
        return parser.parseKqlQuery(kqlQuery, kqlParserContext);
    }

    protected static void assertMultiMatchQuery(QueryBuilder query, String expectedValue, MultiMatchQueryBuilder.Type expectedType) {
        MultiMatchQueryBuilder multiMatchQuery = asInstanceOf(MultiMatchQueryBuilder.class, query);
        assertThat(multiMatchQuery.fields(), anEmptyMap());
        assertThat(multiMatchQuery.lenient(), equalTo(true));
        assertThat(multiMatchQuery.type(), equalTo(expectedType));
        assertThat(multiMatchQuery.value(), equalTo(expectedValue));
    }

    protected static void assertQueryStringBuilder(QueryBuilder query, String expectedFieldName, String expectedValue) {
        QueryStringQueryBuilder queryStringQuery = asInstanceOf(QueryStringQueryBuilder.class, query);
        assertThat(queryStringQuery.queryString(), equalTo(expectedValue));
        assertThat(queryStringQuery.fields().keySet(), contains(expectedFieldName));
    }

    protected static void assertQueryStringBuilder(QueryBuilder query, String expectedValue) {
        QueryStringQueryBuilder queryStringQuery = asInstanceOf(QueryStringQueryBuilder.class, query);
        assertThat(queryStringQuery.queryString(), equalTo(expectedValue));
        assertThat(queryStringQuery.fields(), anEmptyMap());
    }

    protected static void assertTermQueryBuilder(QueryBuilder queryBuilder, String expectedFieldName, String expectedValue) {
        TermQueryBuilder termQuery = asInstanceOf(TermQueryBuilder.class, queryBuilder);
        assertThat(termQuery.fieldName(), equalTo(expectedFieldName));
        assertThat(termQuery.value(), equalTo(expectedValue));
    }

    protected static void assertMatchQueryBuilder(QueryBuilder queryBuilder, String expectedFieldName, String expectedValue) {
        MatchQueryBuilder matchQuery = asInstanceOf(MatchQueryBuilder.class, queryBuilder);
        assertThat(matchQuery.fieldName(), equalTo(expectedFieldName));
        assertThat(matchQuery.value(), equalTo(expectedValue));
    }

    protected static void assertMatchPhraseBuilder(QueryBuilder queryBuilder, String expectedFieldName, String expectedValue) {
        MatchPhraseQueryBuilder matchQuery = asInstanceOf(MatchPhraseQueryBuilder.class, queryBuilder);
        assertThat(matchQuery.fieldName(), equalTo(expectedFieldName));
        assertThat(matchQuery.value(), equalTo(expectedValue));
    }

    protected static void assertWildcardQueryBuilder(QueryBuilder queryBuilder, String expectedFieldName, String expectedValue) {
        WildcardQueryBuilder matchQuery = asInstanceOf(WildcardQueryBuilder.class, queryBuilder);
        assertThat(matchQuery.fieldName(), equalTo(expectedFieldName));
        assertThat(matchQuery.value(), equalTo(expectedValue));
    }

    protected static void assertRangeQueryBuilder(
        QueryBuilder queryBuilder,
        String expectedFieldName,
        Consumer<RangeQueryBuilder> codeBlock
    ) {
        RangeQueryBuilder rangeQuery = asInstanceOf(RangeQueryBuilder.class, queryBuilder);
        assertThat(rangeQuery.fieldName(), equalTo(expectedFieldName));
        codeBlock.accept(rangeQuery);
    }
}
