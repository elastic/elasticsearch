/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.querydsl.query;

import org.apache.lucene.search.BoostQuery;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.MatchNoDocsQuery;
import org.apache.lucene.search.Query;
import org.elasticsearch.common.lucene.search.Queries;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.compute.querydsl.query.QueryWarnings;
import org.elasticsearch.index.IndexMode;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.index.mapper.DocumentMapper;
import org.elasticsearch.index.mapper.MapperService;
import org.elasticsearch.index.mapper.MapperServiceTestCase;
import org.elasticsearch.index.mapper.flattened.FlattenedFieldMapper;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.index.query.Rewriteable;
import org.elasticsearch.index.query.SearchExecutionContext;
import org.elasticsearch.xpack.esql.plugin.EsqlSearchExecutionContext;

import java.io.IOException;
import java.util.List;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.instanceOf;

/**
 * Each shard decides whether it runs the query over fields some shards may not map, or matches every document for the filter
 * to check.
 */
public class UnmappedFieldsQueryBuilderTests extends MapperServiceTestCase {

    public void testRunsOnAPlainKeyword() throws IOException {
        assertThat(isFallback(toQuery("""
            { "properties": { "k": { "type": "keyword" } } }""", "k", false)), equalTo(false));
    }

    public void testMatchesEveryDocumentOnAKeywordWithoutDocValues() throws IOException {
        assertThat(isFallback(toQuery("""
            { "properties": { "k": { "type": "keyword", "doc_values": false } } }""", "k", false)), equalTo(true));
    }

    public void testRunsOnTheKeyOfAFlattenedField() throws IOException {
        String mapping = """
            { "properties": { "attrs": { "type": "flattened" } } }""";
        assertThat(isFallback(toQuery(mapping, "attrs.k", true)), equalTo(false));
        assertThat(isFallback(toQuery(mapping, "attrs.k", false)), equalTo(false));
    }

    public void testMatchesEveryDocumentWhereTheFieldMayBeUnmapped() throws IOException {
        assertThat(isFallback(toQuery("""
            { "dynamic": false, "properties": { "other": { "type": "keyword" } } }""", "k", false)), equalTo(true));
    }

    public void testRunsWhereTheFieldCannotBeUnmapped() throws IOException {
        Query query = toQuery("""
            { "dynamic": "strict", "properties": { "other": { "type": "keyword" } } }""", "k", false);
        assertThat(query, instanceOf(MatchNoDocsQuery.class));
    }

    public void testRunsOnTheUnmappedFieldsTheIndexKeeps() throws IOException {
        assumeTrue("flattened_unmapped_fields is enabled", FlattenedFieldMapper.UNMAPPED_FIELDS_FEATURE_FLAG.isEnabled());
        Settings settings = Settings.builder()
            .put(IndexSettings.MODE.getKey(), IndexMode.COLUMNAR.getName())
            .put(IndexSettings.FLATTENED_UNMAPPED_FIELDS_ENABLED.getKey(), true)
            .build();
        MapperService mapperService = createMapperService(settings, mapping(b -> {}));
        DocumentMapper mapper = mapperService.documentMapper();
        withLuceneIndex(mapperService, iw -> {
            iw.addDocuments(mapper.parse(source(b -> b.field("k", "a"))).docs());
            iw.addDocuments(mapper.parse(source(b -> b.field("k", "b"))).docs());
        }, reader -> {
            IndexSearcher searcher = new IndexSearcher(reader);
            var context = new EsqlSearchExecutionContext(createSearchExecutionContext(mapperService, searcher), QueryWarnings.NOOP);
            Query query = toQuery(context, "k");
            assertThat(isFallback(query), equalTo(false));
            assertThat(query.toString(), searcher.count(query), equalTo(1));
        });
    }

    /** Whether {@code query} is the fallback that matches every document and scores nothing. */
    private static boolean isFallback(Query query) {
        return query instanceof BoostQuery boost && boost.getBoost() == 0f && boost.getQuery() == Queries.ALL_DOCS_INSTANCE;
    }

    private Query toQuery(String mapping, String field, boolean flattenedKeys) throws IOException {
        MapperService mapperService = createMapperService("{ \"_doc\": " + mapping + " }");
        Query[] query = new Query[1];
        withLuceneIndex(mapperService, iw -> {}, reader -> {
            SearchExecutionContext base = createSearchExecutionContext(mapperService, new IndexSearcher(reader));
            query[0] = toQuery(new EsqlSearchExecutionContext(base, QueryWarnings.NOOP, flattenedKeys), field);
        });
        return query[0];
    }

    /** The query a shard runs for {@code field == "a"}, rewritten first as every shard does. */
    private static Query toQuery(SearchExecutionContext context, String field) throws IOException {
        QueryBuilder builder = new UnmappedFieldsQueryBuilder(List.of(field), QueryBuilders.termQuery(field, "a"));
        return Rewriteable.rewrite(builder, context, true).toQuery(context);
    }
}
