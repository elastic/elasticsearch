/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.ml.query;

import org.apache.lucene.search.BooleanClause;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.Query;
import org.elasticsearch.action.admin.indices.mapping.put.PutMappingRequest;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.compress.CompressedXContent;
import org.elasticsearch.index.mapper.MapperService;
import org.elasticsearch.index.mapper.extras.MapperExtrasPlugin;
import org.elasticsearch.index.query.MatchNoneQueryBuilder;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.SearchExecutionContext;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.test.AbstractQueryTestCase;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.ml.MachineLearning;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.instanceOf;

public class SparseTermsQueryBuilderTests extends AbstractQueryTestCase<SparseTermsQueryBuilder> {

    private static final String TEST_FIELD_NAME = "sparse_terms_field";

    private static SparseTermsQueryBuilder createTestQueryBuilder(String fieldName) {
        int numTerms = randomIntBetween(1, 500);
        return new SparseTermsQueryBuilder(
            fieldName,
            randomList(numTerms, numTerms, () -> randomAlphaOfLength(10)),
            randomList(numTerms, numTerms, ESTestCase::randomFloat)
        );
    }

    @Override
    protected void initializeAdditionalMappings(MapperService mapperService) throws IOException {
        mapperService.merge(
            "_doc",
            new CompressedXContent(Strings.toString(PutMappingRequest.simpleMapping(TEST_FIELD_NAME, "type=rank_features"))),
            MapperService.MergeReason.MAPPING_UPDATE
        );
    }

    @Override
    protected Collection<Class<? extends Plugin>> getPlugins() {
        return Arrays.asList(MachineLearning.class, MapperExtrasPlugin.class);
    }

    @Override
    protected SparseTermsQueryBuilder doCreateTestQueryBuilder() {
        return createTestQueryBuilder(TEST_FIELD_NAME);
    }

    @Override
    protected void doAssertLuceneQuery(SparseTermsQueryBuilder queryBuilder, Query query, SearchExecutionContext context) {
        assertThat(query, instanceOf(BooleanQuery.class));
        BooleanQuery booleanQuery = (BooleanQuery) query;
        assertThat(booleanQuery.clauses().size(), equalTo(queryBuilder.getTerms().size()));
        assertThat(booleanQuery.getMinimumNumberShouldMatch(), equalTo(1));
        for (BooleanClause clause : booleanQuery.clauses()) {
            assertThat(clause.getOccur(), equalTo(BooleanClause.Occur.SHOULD));
            assertThat(clause.getQuery(), instanceOf(SparseTermsQuery.class));
        }
    }

    @Override
    public void testMustRewrite() {
        SearchExecutionContext context = createSearchExecutionContext();
        context.setAllowUnmappedFields(true);
        SparseTermsQueryBuilder builder = createTestQueryBuilder("unmapped_field");
        IllegalStateException e = expectThrows(IllegalStateException.class, () -> builder.toQuery(context));
        assertEquals("Rewrite first", e.getMessage());

        QueryBuilder rewrite = rewriteAndFetch(builder, createSearchExecutionContext());
        assertThat(rewrite, instanceOf(MatchNoneQueryBuilder.class));

        rewrite = rewriteAndFetch(createTestQueryBuilder(TEST_FIELD_NAME), createSearchExecutionContext());
        assertThat(rewrite, instanceOf(SparseTermsQueryBuilder.class));

        rewrite = rewriteAndFetch(new SparseTermsQueryBuilder(TEST_FIELD_NAME, List.of(), List.of()), createSearchExecutionContext());
        assertThat(rewrite, instanceOf(MatchNoneQueryBuilder.class));
    }

    public void testIllegalConstructionArguments() {
        IllegalArgumentException ex = expectThrows(
            IllegalArgumentException.class,
            () -> new SparseTermsQueryBuilder("foo", List.of("foo"), List.of(1f, 2f))
        );
        assertThat(ex.getMessage(), equalTo("[terms] length must equal [scores]"));

        expectThrows(IllegalArgumentException.class, () -> new SparseTermsQueryBuilder(null, List.of("foo"), List.of(1f)));

        expectThrows(IllegalArgumentException.class, () -> new SparseTermsQueryBuilder("foo", null, List.of(1f)));

        expectThrows(IllegalArgumentException.class, () -> new SparseTermsQueryBuilder("foo", List.of("foo"), null));
    }
}
