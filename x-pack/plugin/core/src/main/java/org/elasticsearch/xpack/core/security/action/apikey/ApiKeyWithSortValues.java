/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.core.security.action.apikey;

import org.elasticsearch.core.Nullable;
import org.elasticsearch.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.Arrays;

public class ApiKeyWithSortValues extends ApiKey {

    @Nullable
    private final Object[] sortValues;

    public ApiKeyWithSortValues(ApiKey apiKey, @Nullable Object[] sortValues) {
        super(apiKey);
        this.sortValues = sortValues;
    }

    @Nullable
    public Object[] getSortValues() {
        return sortValues;
    }

    @Override
    public void innerToXContent(XContentBuilder builder, Params params) throws IOException {
        super.innerToXContent(builder, params);
        if (sortValues != null && sortValues.length > 0) {
            builder.array("_sort", sortValues);
        }
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        return super.equals(o) && Arrays.equals(sortValues, ((ApiKeyWithSortValues) o).getSortValues());
    }

    @Override
    public int hashCode() {
        int result = super.hashCode();
        result = 31 * result + Arrays.hashCode(sortValues);
        return result;
    }

    @Override
    public String toString() {
        return "ApiKeyWithSortValues [apiKey=" + super.toString() + ", sortValues=" + Arrays.toString(sortValues) + ']';
    }
}
