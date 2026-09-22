/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.plugin;

import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.xpack.esql.VerificationException;
import org.elasticsearch.xpack.esql.action.AbstractEsqlIntegTestCase;
import org.elasticsearch.xpack.esql.action.EsqlCapabilities;

import static org.hamcrest.CoreMatchers.containsString;

/**
 * These integration tests only test scenarios which cannot be tested by CSV tests. Specifically, errors during query verification.
 */
public class KnnRuntimeFunctionIT extends AbstractEsqlIntegTestCase {

    @Override
    protected QueryPragmas getPragmas() {
        if (canUseQueryPragmas() == false) {
            return QueryPragmas.EMPTY;
        }
        return new QueryPragmas(Settings.builder().put(QueryPragmas.KNN_RUNTIME_FIELD.getKey(), true).build());
    }

    public void testKnnRuntimeNonUnitQueryVector() {
        assumeTrue("requires query pragmas", canUseQueryPragmas());
        assumeTrue("requires runtime search support", EsqlCapabilities.Cap.KNN_RUNTIME_FIELD.isEnabled());

        var query = """
            ROW dv = to_dense_vector([1.0, 0.0, 0.0])
            | WHERE knn(dv, [0.0, 2.0, 0.0], {"similarity_function": "dot_product"})
            | KEEP dv
            | LIMIT 10
            """;

        var error = expectThrows(VerificationException.class, () -> run(query));
        assertThat(
            error.getMessage(),
            containsString("[KNN] dot_product requires unit-length vectors; provided query vector has magnitude [2.0]")
        );
    }

    public void testKnnRuntimeZeroMagnitudeQueryVector() {
        assumeTrue("requires query pragmas", canUseQueryPragmas());
        assumeTrue("requires runtime search support", EsqlCapabilities.Cap.KNN_RUNTIME_FIELD.isEnabled());

        var query = """
            ROW dv = to_dense_vector([1.0, 0.0, 0.0])
            | WHERE knn(dv, [0.0, 0.0, 0.0], {"similarity_function": "cosine"})
            | KEEP dv
            | LIMIT 10
            """;

        var error = expectThrows(VerificationException.class, () -> run(query));
        assertThat(
            error.getMessage(),
            containsString("[KNN] cannot operate on provided query vector; Cosine similarity does not support (query) vectors with zero magnitude.")
        );
    }
}
