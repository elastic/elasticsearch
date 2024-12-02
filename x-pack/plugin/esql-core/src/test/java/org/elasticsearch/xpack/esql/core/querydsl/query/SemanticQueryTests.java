/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.core.querydsl.query;

import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.NestedQueryBuilder;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.TermQueryBuilder;
import org.elasticsearch.inference.InferenceResults;
import org.elasticsearch.search.vectors.KnnVectorQueryBuilder;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.core.ml.inference.results.MlTextEmbeddingResults;
import org.elasticsearch.xpack.core.ml.inference.results.TextExpansionResults;
import org.elasticsearch.xpack.core.ml.search.WeightedToken;
import org.elasticsearch.xpack.esql.core.expression.FieldAttribute;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.tree.SourceTests;
import org.elasticsearch.xpack.esql.core.type.EsField;
import org.elasticsearch.xpack.esql.core.util.StringUtils;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.function.Function;

import static java.util.Collections.emptyMap;
import static org.elasticsearch.test.EqualsHashCodeTestUtils.checkEqualsAndHashCode;
import static org.elasticsearch.xpack.core.ml.inference.trainedmodel.InferenceConfig.DEFAULT_RESULTS_FIELD;
import static org.elasticsearch.xpack.esql.core.tree.Source.EMPTY;
import static org.elasticsearch.xpack.esql.core.type.DataType.SEMANTIC_TEXT;

public class SemanticQueryTests extends ESTestCase {

    public void testQueryBuildingWithSparseEmbeddings() {
        TextExpansionResults textExpansionResults = randomTextExpansionResults();

        SemanticQuery semanticQuery = new SemanticQuery(SourceTests.randomSource(), "title", "something", textExpansionResults);

        NestedQueryBuilder nestedQueryBuilder = asInstanceOf(NestedQueryBuilder.class, semanticQuery.asBuilder());

        assertEquals("title.inference.chunks", nestedQueryBuilder.path());
        BoolQueryBuilder boolQuery = asInstanceOf(BoolQueryBuilder.class, nestedQueryBuilder.query());
        assertEquals(boolQuery.should().size(), textExpansionResults.getWeightedTokens().size());
        for (QueryBuilder shouldClause : boolQuery.should()) {
            TermQueryBuilder termQueryBuilder = asInstanceOf(TermQueryBuilder.class, shouldClause);
            assertEquals("title.inference.chunks.embeddings", termQueryBuilder.fieldName());
        }
    }

    public void testQueryBuildingWithDenseEmbeddings() {
        MlTextEmbeddingResults textEmbeddingResults = randomMlTextEmbeddingsResults();

        SemanticQuery semanticQuery = new SemanticQuery(SourceTests.randomSource(), "title", "something", textEmbeddingResults);

        NestedQueryBuilder nestedQueryBuilder = asInstanceOf(NestedQueryBuilder.class, semanticQuery.asBuilder());

        assertEquals("title.inference.chunks", nestedQueryBuilder.path());
        KnnVectorQueryBuilder expected = new KnnVectorQueryBuilder(
            "title.inference.chunks.embeddings",
            textEmbeddingResults.getInferenceAsFloat(),
            null,
            null,
            null
        );
        assertEquals(expected, nestedQueryBuilder.query());
    }

    public void testEqualsAndHashCode() {
        checkEqualsAndHashCode(randomSemanticQuery(), SemanticQueryTests::copy, SemanticQueryTests::mutate);
    }

    private static SemanticQuery copy(SemanticQuery query) {
        return new SemanticQuery(query.source(), query.name(), query.text(), query.inferenceResults());
    }

    private static SemanticQuery mutate(SemanticQuery query) {
        List<Function<SemanticQuery, SemanticQuery>> options = Arrays.asList(
            q -> new SemanticQuery(SourceTests.mutate(q.source()), q.name(), q.text(), q.inferenceResults()),
            q -> new SemanticQuery(
                q.source(),
                randomValueOtherThan(q.name(), () -> randomAlphaOfLength(5)),
                q.text(),
                q.inferenceResults()
            ),
            q -> new SemanticQuery(
                q.source(),
                q.name(),
                randomValueOtherThan(q.text(), () -> randomAlphaOfLength(5)),
                q.inferenceResults()
            ),
            q -> new SemanticQuery(
                q.source(),
                q.name(),
                q.text(),
                randomValueOtherThan(q.inferenceResults(), () -> randomInferenceResults())
            )
        );

        return randomFrom(options).apply(query);
    }

    public void testToString() {
        final Source source = new Source(1, 1, StringUtils.EMPTY);
        FieldAttribute fa = new FieldAttribute(EMPTY, "a", new EsField("af", SEMANTIC_TEXT, emptyMap(), true));

        SemanticQuery semanticQuery = new SemanticQuery(source, "title", "foo", null);
        assertEquals("SemanticQuery@1:2[title:foo]", semanticQuery.toString());
    }

    public static InferenceResults randomInferenceResults() {
        if (randomBoolean()) {
            return randomTextExpansionResults();
        }
        return randomMlTextEmbeddingsResults();
    }

    private static TextExpansionResults randomTextExpansionResults() {
        int numTokens = randomIntBetween(5, 50);
        List<WeightedToken> tokenList = new ArrayList<>();
        for (int i = 0; i < numTokens; i++) {
            tokenList.add(new WeightedToken(Integer.toString(i), (float) randomDoubleBetween(0.0, 5.0, false)));
        }
        return new TextExpansionResults(randomAlphaOfLength(4), tokenList, randomBoolean());
    }

    private static MlTextEmbeddingResults randomMlTextEmbeddingsResults() {
        int columns = randomIntBetween(1, 10);
        double[] arr = new double[columns];
        for (int i = 0; i < columns; i++) {
            arr[i] = randomDouble();
        }
        return new MlTextEmbeddingResults(DEFAULT_RESULTS_FIELD, arr, randomBoolean());
    }

    private static SemanticQuery randomSemanticQuery() {
        return new SemanticQuery(SourceTests.randomSource(), randomAlphaOfLength(5), randomAlphaOfLength(5), randomInferenceResults());
    }
}
