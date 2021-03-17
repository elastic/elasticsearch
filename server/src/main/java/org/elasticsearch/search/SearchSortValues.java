/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.search;

import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.lucene.Lucene;
import org.elasticsearch.common.xcontent.ToXContentFragment;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.common.xcontent.XContentParserUtils;
import org.elasticsearch.search.SearchHit.Fields;

import java.io.IOException;
import java.util.Arrays;
import java.util.Objects;

public class SearchSortValues implements ToXContentFragment, Writeable {

    private static final Object[] EMPTY_ARRAY = new Object[0];
    static final SearchSortValues EMPTY = new SearchSortValues(EMPTY_ARRAY);

    private final Object[] formattedSortValues;
    private final Object[] rawSortValues;

    SearchSortValues(Object[] sortValues) {
        this.formattedSortValues = Objects.requireNonNull(sortValues, "sort values must not be empty");
        this.rawSortValues = EMPTY_ARRAY;
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
            assert v == null || v instanceof String || v instanceof Number || v instanceof Boolean: v + " was not formatted";
            formattedSortValues[i] = v;
        }
    }

    SearchSortValues(StreamInput in) throws IOException {
        this.formattedSortValues = in.readArray(Lucene::readSortValue, Object[]::new);
        this.rawSortValues = in.readArray(Lucene::readSortValue, Object[]::new);
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

    public static SearchSortValues fromXContent(XContentParser parser) throws IOException {
        XContentParserUtils.ensureExpectedToken(XContentParser.Token.START_ARRAY, parser.currentToken(), parser);
        return new SearchSortValues(parser.list().toArray());
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
        return Arrays.equals(formattedSortValues, that.formattedSortValues) &&
            Arrays.equals(rawSortValues, that.rawSortValues);
    }

    @Override
    public int hashCode() {
        int result = Arrays.hashCode(formattedSortValues);
        result = 31 * result + Arrays.hashCode(rawSortValues);
        return result;
    }
}
