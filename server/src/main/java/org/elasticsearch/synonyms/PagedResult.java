/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.synonyms;

import org.elasticsearch.common.io.stream.Writeable;

import java.util.Arrays;
import java.util.Objects;

/**
 * A paged result that includes total number of results and the results for the current page
 *
 * @param totalResults total number of results
 * @param pageResults results for current page
 * @param <T> type of results
 */
public record PagedResult<T extends Writeable>(long totalResults, T[] pageResults) {
    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        @SuppressWarnings("unchecked")
        PagedResult<T> that = (PagedResult<T>) o;
        return totalResults == that.totalResults && Arrays.equals(pageResults, that.pageResults);
    }

    @Override
    public int hashCode() {
        int result = Objects.hash(totalResults);
        result = 31 * result + Arrays.hashCode(pageResults);
        return result;
    }
}
