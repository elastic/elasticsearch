/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.search.vectors;

import org.apache.lucene.search.BooleanClause;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.KnnVectorQuery;
import org.apache.lucene.search.Query;
import org.elasticsearch.Version;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.compress.CompressedXContent;
import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.elasticsearch.common.io.stream.NamedWriteableAwareStreamInput;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.index.mapper.MapperService;
import org.elasticsearch.index.query.MatchNoneQueryBuilder;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.index.query.SearchExecutionContext;
import org.elasticsearch.index.query.TermQueryBuilder;
import org.elasticsearch.test.AbstractBuilderTestCase;
import org.elasticsearch.test.AbstractQueryTestCase;
import org.elasticsearch.test.VersionUtils;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.instanceOf;

public class KnnVectorQueryBuilderTests extends AbstractQueryTestCase<KnnVectorQueryBuilder> {
    private static final String VECTOR_FIELD = "vector";
    private static final String VECTOR_ALIAS_FIELD = "vector_alias";
    private static final int VECTOR_DIMENSION = 3;

    @Override
    protected void initializeAdditionalMappings(MapperService mapperService) throws IOException {
        XContentBuilder builder = XContentFactory.jsonBuilder()
            .startObject()
            .startObject("properties")
            .startObject(VECTOR_FIELD)
            .field("type", "dense_vector")
            .field("dims", VECTOR_DIMENSION)
            .field("index", true)
            .field("similarity", "l2_norm")
            .endObject()
            .startObject(VECTOR_ALIAS_FIELD)
            .field("type", "alias")
            .field("path", VECTOR_FIELD)
            .endObject()
            .endObject()
            .endObject();
        mapperService.merge(
            MapperService.SINGLE_MAPPING_NAME,
            new CompressedXContent(Strings.toString(builder)),
            MapperService.MergeReason.MAPPING_UPDATE
        );
    }

    @Override
    protected KnnVectorQueryBuilder doCreateTestQueryBuilder() {
        String fieldName = randomBoolean() ? VECTOR_FIELD : VECTOR_ALIAS_FIELD;
        float[] vector = new float[VECTOR_DIMENSION];
        for (int i = 0; i < vector.length; i++) {
            vector[i] = randomFloat();
        }
        int numCands = randomIntBetween(1, 1000);

        KnnVectorQueryBuilder queryBuilder = new KnnVectorQueryBuilder(fieldName, vector, numCands);

        if (randomBoolean()) {
            List<QueryBuilder> filters = new ArrayList<>();
            int numFilters = randomIntBetween(1, 5);
            for (int i = 0; i < numFilters; i++) {
                String filterFieldName = randomBoolean() ? KEYWORD_FIELD_NAME : TEXT_FIELD_NAME;
                filters.add(QueryBuilders.termQuery(filterFieldName, randomAlphaOfLength(10)));
            }
            queryBuilder.addFilterQueries(filters);
        }
        return queryBuilder;
    }

    @Override
    protected void doAssertLuceneQuery(KnnVectorQueryBuilder queryBuilder, Query query, SearchExecutionContext context) throws IOException {
        assertTrue(query instanceof KnnVectorQuery);
        KnnVectorQuery knnVectorQuery = (KnnVectorQuery) query;

        BooleanQuery.Builder builder = new BooleanQuery.Builder();
        for (QueryBuilder qb : queryBuilder.filterQueries()) {
            builder.add(qb.toQuery(context), BooleanClause.Occur.FILTER);
        }
        BooleanQuery booleanQuery = builder.build();
        Query filterQuery = booleanQuery.clauses().isEmpty() ? null : booleanQuery;
        // The field should always be resolved to the concrete field
        Query knnVectorQueryBuilt = new KnnVectorQuery(VECTOR_FIELD, queryBuilder.queryVector(), queryBuilder.numCands(), filterQuery);
        assertEquals(knnVectorQuery, knnVectorQueryBuilt);
    }

    public void testWrongDimension() {
        SearchExecutionContext context = createSearchExecutionContext();
        KnnVectorQueryBuilder query = new KnnVectorQueryBuilder(VECTOR_FIELD, new float[] { 1.0f, 2.0f }, 10);
        IllegalArgumentException e = expectThrows(IllegalArgumentException.class, () -> query.doToQuery(context));
        assertThat(e.getMessage(), containsString("the query vector has a different dimension [2] than the index vectors [3]"));
    }

    public void testNonexistentField() {
        SearchExecutionContext context = createSearchExecutionContext();
        KnnVectorQueryBuilder query = new KnnVectorQueryBuilder("nonexistent", new float[] { 1.0f, 1.0f, 1.0f }, 10);
        IllegalArgumentException e = expectThrows(IllegalArgumentException.class, () -> query.doToQuery(context));
        assertThat(e.getMessage(), containsString("field [nonexistent] does not exist in the mapping"));
    }

    public void testWrongFieldType() {
        SearchExecutionContext context = createSearchExecutionContext();
        KnnVectorQueryBuilder query = new KnnVectorQueryBuilder(
            AbstractBuilderTestCase.KEYWORD_FIELD_NAME,
            new float[] { 1.0f, 1.0f, 1.0f },
            10
        );
        IllegalArgumentException e = expectThrows(IllegalArgumentException.class, () -> query.doToQuery(context));
        assertThat(e.getMessage(), containsString("[knn] queries are only supported on [dense_vector] fields"));
    }

    @Override
    public void testValidOutput() {
        KnnVectorQueryBuilder query = new KnnVectorQueryBuilder(VECTOR_FIELD, new float[] { 1.0f, 2.0f, 3.0f }, 10);
        String expected = """
            {
              "knn" : {
                "field" : "vector",
                "vector" : [
                  1.0,
                  2.0,
                  3.0
                ],
                "num_candidates" : 10
              }
            }""";
        assertEquals(expected, query.toString());
    }

    @Override
    public void testMustRewrite() throws IOException {
        SearchExecutionContext context = createSearchExecutionContext();
        context.setAllowUnmappedFields(true);
        TermQueryBuilder termQuery = new TermQueryBuilder("unmapped_field", 42);
        KnnVectorQueryBuilder query = new KnnVectorQueryBuilder(VECTOR_FIELD, new float[] { 1.0f, 2.0f, 3.0f }, VECTOR_DIMENSION);
        query.addFilterQuery(termQuery);

        IllegalStateException e = expectThrows(IllegalStateException.class, () -> query.toQuery(context));
        assertEquals("Rewrite first", e.getMessage());

        QueryBuilder rewrittenQuery = query.rewrite(context);
        assertThat(rewrittenQuery, instanceOf(MatchNoneQueryBuilder.class));
    }

    public void testOldVersionSerialization() throws IOException {
        KnnVectorQueryBuilder query = createTestQueryBuilder();
        KnnVectorQueryBuilder queryWithNoFilters = new KnnVectorQueryBuilder(query.getFieldName(), query.queryVector(), query.numCands());
        queryWithNoFilters.queryName(query.queryName()).boost(query.boost());

        Version newVersion = VersionUtils.randomVersionBetween(random(), Version.V_8_2_0, Version.CURRENT);
        Version oldVersion = VersionUtils.randomVersionBetween(random(), Version.V_8_0_0, Version.V_8_1_0);

        assertSerialization(query, newVersion);
        assertSerialization(queryWithNoFilters, oldVersion);

        try (BytesStreamOutput output = new BytesStreamOutput()) {
            output.setVersion(newVersion);
            output.writeNamedWriteable(query);
            try (StreamInput in = new NamedWriteableAwareStreamInput(output.bytes().streamInput(), namedWriteableRegistry())) {
                in.setVersion(oldVersion);
                KnnVectorQueryBuilder deserializedQuery = (KnnVectorQueryBuilder) in.readNamedWriteable(QueryBuilder.class);
                assertEquals(queryWithNoFilters, deserializedQuery);
                assertEquals(queryWithNoFilters.hashCode(), deserializedQuery.hashCode());
            }
        }
    }

    @Override
    public void testUnknownObjectException() throws IOException {
        // Test isn't relevant, since query is never parsed from xContent
    }

    @Override
    public void testFromXContent() throws IOException {
        // Test isn't relevant, since query is never parsed from xContent
    }

    @Override
    public void testUnknownField() throws IOException {
        // Test isn't relevant, since query is never parsed from xContent
    }
}
