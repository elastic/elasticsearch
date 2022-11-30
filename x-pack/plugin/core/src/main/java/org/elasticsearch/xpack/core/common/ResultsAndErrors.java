/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.core.common;

import java.util.Collection;
import java.util.List;
import java.util.Map;

/**
 * A record class encapsulate a collection of results and associated errors. An intended usage is to model the
 * generic MultiGetResponse to domain specific ones. The results are a collection of entity objects translated
 * from the documents retrieved by MultiGet and the errors are a map key by IDs and any exception encountered
 * when attempt retrieving associated documents.
 */
public record ResultsAndErrors<T> (Collection<T> results, Map<String, Exception> errors) {

    private static final ResultsAndErrors<?> EMPTY = new ResultsAndErrors<>(List.of(), Map.of());

    public boolean isEmpty() {
        return results.isEmpty() && errors.isEmpty();
    }

    @SuppressWarnings("unchecked")
    public static <T> ResultsAndErrors<T> empty() {
        return (ResultsAndErrors<T>) EMPTY;
    }

}
