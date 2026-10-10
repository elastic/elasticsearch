/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.querydsl.query;

import org.apache.lucene.search.IndexSearcher;
import org.elasticsearch.compute.querydsl.query.QueryWarnings;
import org.elasticsearch.index.mapper.MapperService;
import org.elasticsearch.index.mapper.MapperServiceTestCase;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.SearchExecutionContext;
import org.elasticsearch.xpack.esql.core.querydsl.query.ExistsQuery;
import org.elasticsearch.xpack.esql.core.querydsl.query.NotQuery;
import org.elasticsearch.xpack.esql.core.querydsl.query.Query;
import org.elasticsearch.xpack.esql.core.querydsl.query.TermQuery;
import org.elasticsearch.xpack.esql.core.querydsl.query.WildcardQuery;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.plugin.EsqlSearchExecutionContext;

import java.io.IOException;

import static org.hamcrest.Matchers.equalTo;

/**
 * A shard where {@code category} is {@code flattened} resolves {@code category.raw} to a keyed sub-field type,
 * but field caps and field extraction treat {@code category.raw} as unmapped there. This happens when another
 * index in the same {@code FROM} maps {@code category.raw} as a {@code keyword} multi-field. A
 * {@link SingleValueQuery} or {@link ExistsQuery} on that name must then behave as on a missing field.
 */
public class FlattenedSubkeyPushdownTests extends MapperServiceTestCase {

    public void testTermOnDynamicSubkeyMatchesNothing() throws IOException {
        assertThat(count(new SingleValueQuery(new TermQuery(Source.EMPTY, "category.raw", "alpha"), "category.raw", false)), equalTo(0));
    }

    public void testNegatedTermOnDynamicSubkeyMatchesNothing() throws IOException {
        SingleValueQuery query = new SingleValueQuery(new TermQuery(Source.EMPTY, "category.raw", "alpha"), "category.raw", false);
        assertThat(count(query.negate(Source.EMPTY)), equalTo(0));
    }

    public void testWildcardOnDynamicSubkeyMatchesNothing() throws IOException {
        WildcardQuery wildcard = new WildcardQuery(Source.EMPTY, "category.raw", "*test*", false, false);
        assertThat(count(new SingleValueQuery(wildcard, "category.raw", false)), equalTo(0));
        assertThat(count(new SingleValueQuery(wildcard, "category.raw", false).negate(Source.EMPTY)), equalTo(0));
    }

    public void testMappedMultiFieldStillMatches() throws IOException {
        assertThat(count(new SingleValueQuery(new TermQuery(Source.EMPTY, "kw.raw", "alpha"), "kw.raw", false)), equalTo(1));
    }

    public void testExistsOnDynamicSubkeyMatchesNothing() throws IOException {
        assertThat(count(new ExistsQuery(Source.EMPTY, "category.raw")), equalTo(0));
    }

    public void testNotExistsOnDynamicSubkeyMatchesEverything() throws IOException {
        assertThat(count(new NotQuery(Source.EMPTY, new ExistsQuery(Source.EMPTY, "category.raw"))), equalTo(2));
    }

    public void testExistsOnMappedMultiFieldStillMatches() throws IOException {
        assertThat(count(new ExistsQuery(Source.EMPTY, "kw.raw")), equalTo(2));
    }

    private int count(Query query) throws IOException {
        MapperService mapper = createMapperService(mapping(b -> {
            b.startObject("category").field("type", "flattened").endObject();
            b.startObject("kw").field("type", "text");
            b.startObject("fields").startObject("raw").field("type", "keyword").endObject().endObject();
            b.endObject();
        }));
        int[] count = new int[1];
        withLuceneIndex(mapper, iw -> {
            iw.addDocument(
                mapper.documentMapper()
                    .parse(source(b -> b.startObject("category").field("raw", "alpha").endObject().field("kw", "alpha")))
                    .rootDoc()
            );
            iw.addDocument(
                mapper.documentMapper()
                    .parse(source(b -> b.startObject("category").field("other", "x").endObject().field("kw", "beta")))
                    .rootDoc()
            );
        }, reader -> {
            SearchExecutionContext ctx = new EsqlSearchExecutionContext(
                createSearchExecutionContext(mapper, new IndexSearcher(reader)),
                QueryWarnings.EMIT
            );
            QueryBuilder builder = query.toQueryBuilder().rewrite(ctx);
            count[0] = ctx.searcher().count(builder.toQuery(ctx));
        });
        return count[0];
    }
}
