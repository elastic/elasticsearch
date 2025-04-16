/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.core.esql.action.internal;

import org.elasticsearch.client.internal.ElasticsearchClient;
import org.elasticsearch.xpack.core.esql.action.EsqlQueryRequest;
import org.elasticsearch.xpack.core.esql.action.EsqlQueryRequestBuilder;
import org.elasticsearch.xpack.core.esql.action.EsqlQueryResponse;

/**
 * For secret access to ES|QL internals only. Do not use.
 * TODO qualify export when ES|QL is modularized
 */
public class SharedSecrets {

    private static EsqlQueryRequestBuilderAccess esqlQueryRequestBuilderAccess;

    public static void setEsqlQueryRequestBuilderAccess(EsqlQueryRequestBuilderAccess access) {
        esqlQueryRequestBuilderAccess = access;
    }

    public static EsqlQueryRequestBuilderAccess getEsqlQueryRequestBuilderAccess() {
        var access = esqlQueryRequestBuilderAccess;
        if (access == null) {
            throw new IllegalStateException("ESQL module not present or initialized");
        }
        return access;
    }

    public interface EsqlQueryRequestBuilderAccess {

        EsqlQueryRequestBuilder<? extends EsqlQueryRequest, ? extends EsqlQueryResponse> newEsqlQueryRequestBuilder(
            ElasticsearchClient client
        );
    }
}
