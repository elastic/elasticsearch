/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.search;

import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.lucene.Lucene;
import org.elasticsearch.search.SearchHit.Fields;
import org.elasticsearch.xcontent.ToXContentFragment;
import org.elasticsearch.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.Arrays;
import java.util.Map;
import java.util.Objects;

public class SearchSortValues implements ToXContentFragment, Writeable {

    private static final Object[] EMPTY_ARRAY = new Object[0];
    static final SearchSortValues EMPTY = new SearchSortValues(EMPTY_ARRAY);

    private final Object[] formattedSortValues;
    private final Object[] rawSortValues;

    SearchSortValues(Object[] sortValues) {
        this(Objects.requireNonNull(sortValues, "sort values must not be empty"), EMPTY_ARRAY);
    }

    public SearchSortValues(Object[] rawSortValues, DocValueFormat[] sortValueFormats) {
        Objects.requireNonNull(rawSortValues);
        Objects.requireNonNull(sortValueFormats);
        if (rawSortValues.length != sortValueFormats.length) {
            throw new IllegalArgumentException("formattedSortValues and sortValueFormats must hold the same number of items");
        }
        this.rawSortValues = rawSortValues;
        this.formattedSortValues = new Object[rawSortValues.length];
        for (int i = 0; i < rawSortValues.length; ++i) {
            final Object v = sortValueFormats[i].formatSortValue(rawSortValues[i]);
            assert v == null || v instanceof String || v instanceof Number || v instanceof Boolean || v instanceof Map
                : v + " was not formatted";
            formattedSortValues[i] = v;
        }
    }

    public static SearchSortValues readFrom(StreamInput in) throws IOException {
        Object[] formattedSortValues = in.readArray(Lucene::readSortValue, Object[]::new);
        Object[] rawSortValues = in.readArray(Lucene::readSortValue, Object[]::new);
        if (formattedSortValues.length == 0 && rawSortValues.length == 0) {
            return EMPTY;
        }
        return new SearchSortValues(formattedSortValues, rawSortValues);
    }

    private SearchSortValues(Object[] formattedSortValues, Object[] rawSortValues) {
        this.formattedSortValues = formattedSortValues;
        this.rawSortValues = rawSortValues;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeArray(Lucene::writeSortValue, this.formattedSortValues);
        out.writeArray(Lucene::writeSortValue, this.rawSortValues);
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        if (formattedSortValues.length > 0) {
            builder.startArray(Fields.SORT);
            for (Object sortValue : formattedSortValues) {
                builder.value(sortValue);
            }
            builder.endArray();
        }
        return builder;
    }

    /**
     * Returns the formatted version of the values that sorting was performed against
     */
    public Object[] getFormattedSortValues() {
        return formattedSortValues;
    }

    /**
     * Returns the raw version of the values that sorting was performed against
     */
    public Object[] getRawSortValues() {
        return rawSortValues;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        SearchSortValues that = (SearchSortValues) o;
        return Arrays.equals(formattedSortValues, that.formattedSortValues) && Arrays.equals(rawSortValues, that.rawSortValues);
    }

    @Override
    public int hashCode() {
        int result = Arrays.hashCode(formattedSortValues);
        result = 31 * result + Arrays.hashCode(rawSortValues);
        return result;
    }
}
