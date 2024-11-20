/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.external.http.sender;

import java.util.List;
import java.util.Objects;

public class QueryAndDocsInputs extends InferenceInputs {

    public static QueryAndDocsInputs of(InferenceInputs inferenceInputs) {
        if (inferenceInputs instanceof QueryAndDocsInputs == false) {
            throw createUnsupportedTypeException(inferenceInputs);
        }

        return (QueryAndDocsInputs) inferenceInputs;
    }

    public QueryAndDocsInputs(String query, List<String> chunks) {
        this(query, chunks, false);
    }

    public QueryAndDocsInputs(String query, List<String> chunks, boolean stream) {
        super(Objects.requireNonNull(query), Objects.requireNonNull(chunks), stream);
    }

    public String getQuery() {
        return query();
    }

    public List<String> getChunks() {
        return input();
    }

}
