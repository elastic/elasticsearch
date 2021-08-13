/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
package org.elasticsearch.client.rollup;

import org.elasticsearch.action.support.IndicesOptions;
import org.elasticsearch.client.Validatable;
import org.elasticsearch.common.Strings;

import java.util.Arrays;
import java.util.Objects;

public class GetRollupIndexCapsRequest implements Validatable {

    private String[] indices;
    private IndicesOptions options;

    public GetRollupIndexCapsRequest(final String... indices) {
        this(indices, IndicesOptions.STRICT_EXPAND_OPEN_FORBID_CLOSED);
    }

    public GetRollupIndexCapsRequest(final String[] indices, final IndicesOptions options) {
        if (indices == null || indices.length == 0) {
            throw new IllegalArgumentException("[indices] must not be null or empty");
        }
        for (String index : indices) {
            if (Strings.isNullOrEmpty(index)) {
                throw new IllegalArgumentException("[index] must not be null or empty");
            }
        }
        this.indices = indices;
        this.options = Objects.requireNonNull(options);
    }

    public IndicesOptions indicesOptions() {
        return options;
    }

    public String[] indices() {
        return indices;
    }

    @Override
    public int hashCode() {
        return Objects.hash(Arrays.hashCode(indices), options);
    }

    @Override
    public boolean equals(Object obj) {
        if (obj == null) {
            return false;
        }
        if (getClass() != obj.getClass()) {
            return false;
        }
        GetRollupIndexCapsRequest other = (GetRollupIndexCapsRequest) obj;
        return Arrays.equals(indices, other.indices)
            && Objects.equals(options, other.options);
    }
}
