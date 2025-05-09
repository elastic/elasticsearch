/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.search.searchafter;

import org.apache.lucene.search.FieldDoc;
import org.apache.lucene.search.SortField;
import org.apache.lucene.search.SortedNumericSortField;
import org.apache.lucene.search.SortedSetSortField;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.common.ParsingException;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.index.fielddata.IndexFieldData;
import org.elasticsearch.search.DocValueFormat;
import org.elasticsearch.search.sort.SortAndFormats;
import org.elasticsearch.xcontent.ParseField;
import org.elasticsearch.xcontent.Text;
import org.elasticsearch.xcontent.ToXContentObject;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentParser;

import java.io.IOException;
import java.math.BigInteger;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;

public class SearchAfterBuilder implements ToXContentObject, Writeable {
    public static final ParseField SEARCH_AFTER = new ParseField("search_after");
    private static final Object[] EMPTY_SORT_VALUES = new Object[0];

    private Object[] sortValues = EMPTY_SORT_VALUES;

    public SearchAfterBuilder() {}

    /**
     * Read from a stream.
     */
    public SearchAfterBuilder(StreamInput in) throws IOException {
        int size = in.readVInt();
        sortValues = new Object[size];
        for (int i = 0; i < size; i++) {
            sortValues[i] = in.readGenericValue();
        }
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeArray(StreamOutput::writeGenericValue, sortValues);
    }

    public SearchAfterBuilder setSortValues(Object[] values) {
        if (values == null) {
            throw new NullPointerException("Values cannot be null.");
        }
        if (values.length == 0) {
            throw new IllegalArgumentException("Values must contains at least one value.");
        }
        for (int i = 0; i < values.length; i++) {
            if (values[i] == null) continue;
            if (values[i] instanceof String) continue;
            if (values[i] instanceof Text) continue;
            if (values[i] instanceof Long) continue;
            if (values[i] instanceof Integer) continue;
            if (values[i] instanceof Short) continue;
            if (values[i] instanceof Byte) continue;
            if (values[i] instanceof Double) continue;
            if (values[i] instanceof Float) continue;
            if (values[i] instanceof Boolean) continue;
            if (values[i] instanceof BigInteger) continue;
            throw new IllegalArgumentException("Can't handle " + SEARCH_AFTER + " field value of type [" + values[i].getClass() + "]");
        }
        sortValues = new Object[values.length];
        System.arraycopy(values, 0, sortValues, 0, values.length);
        return this;
    }

    public Object[] getSortValues() {
        return Arrays.copyOf(sortValues, sortValues.length);
    }

    public static FieldDoc buildFieldDoc(SortAndFormats sort, Object[] values, @Nullable String collapseField) {
        if (sort == null || sort.sort.getSort() == null || sort.sort.getSort().length == 0) {
            throw new IllegalArgumentException("Sort must contain at least one field.");
        }

        SortField[] sortFields = sort.sort.getSort();
        if (sortFields.length != values.length) {
            throw new IllegalArgumentException(
                SEARCH_AFTER.getPreferredName() + " has " + values.length + " value(s) but sort has " + sort.sort.getSort().length + "."
            );
        }

        if (collapseField != null && (sortFields.length > 1 || Objects.equals(sortFields[0].getField(), collapseField) == false)) {
            throw new IllegalArgumentException(
                "Cannot use [collapse] in conjunction with ["
                    + SEARCH_AFTER.getPreferredName()
                    + "] unless the search is sorted on the same field. Multiple sort fields are not allowed."
            );
        }

        Object[] fieldValues = new Object[sortFields.length];
        for (int i = 0; i < sortFields.length; i++) {
            SortField sortField = sortFields[i];
            DocValueFormat format = sort.formats[i];
            if (values[i] != null) {
                fieldValues[i] = convertValueFromSortField(values[i], sortField, format);
            } else {
                fieldValues[i] = null;
            }
        }
        /*
         * We set the doc id to Integer.MAX_VALUE in order to make sure that the search starts "after" the first document that is equal to
         * the field values.
         */
        return new FieldDoc(Integer.MAX_VALUE, 0, fieldValues);
    }

    /**
     * Returns the inner {@link SortField.Type} expected for this sort field.
     */
    static SortField.Type extractSortType(SortField sortField) {
        if (sortField.getComparatorSource() instanceof IndexFieldData.XFieldComparatorSource) {
            return ((IndexFieldData.XFieldComparatorSource) sortField.getComparatorSource()).reducedType();
        } else if (sortField instanceof SortedSetSortField) {
            return SortField.Type.STRING;
        } else if (sortField instanceof SortedNumericSortField) {
            return ((SortedNumericSortField) sortField).getNumericType();
        } else if ("LatLonPointSortField".equals(sortField.getClass().getSimpleName())) {
            // for geo distance sorting
            return SortField.Type.DOUBLE;
        } else {
            return sortField.getType();
        }
    }

    static Object convertValueFromSortField(Object value, SortField sortField, DocValueFormat format) {
        SortField.Type sortType = extractSortType(sortField);
        return convertValueFromSortType(sortField.getField(), sortType, value, format);
    }

    private static Object convertValueFromSortType(String fieldName, SortField.Type sortType, Object value, DocValueFormat format) {
        try {
            switch (sortType) {
                case DOC, INT:
                    if (value instanceof Number) {
                        return ((Number) value).intValue();
                    }
                    return Integer.parseInt(value.toString());

                case SCORE, FLOAT:
                    if (value instanceof Number) {
                        return ((Number) value).floatValue();
                    }
                    return Float.parseFloat(value.toString());

                case DOUBLE:
                    if (value instanceof Number) {
                        return ((Number) value).doubleValue();
                    }
                    return Double.parseDouble(value.toString());

                case LONG:
                    // for unsigned_long field type we want to pass search_after value through formatting
                    if (value instanceof Number && format != DocValueFormat.UNSIGNED_LONG_SHIFTED) {
                        return ((Number) value).longValue();
                    }
                    return format.parseLong(
                        value.toString(),
                        false,
                        () -> { throw new IllegalStateException("now() is not allowed in [search_after] key"); }
                    );

                case STRING_VAL:
                case STRING:
                    if (value instanceof BytesRef bytesRef) {
                        // _tsid is stored and ordered as BytesRef. We should not format it
                        return bytesRef;
                    } else {
                        return format.parseBytesRef(value);
                    }

                default:
                    throw new IllegalArgumentException(
                        "Comparator type [" + sortType.name() + "] for field [" + fieldName + "] is not supported."
                    );
            }
        } catch (NumberFormatException e) {
            throw new IllegalArgumentException(
                "Failed to parse " + SEARCH_AFTER.getPreferredName() + " value for field [" + fieldName + "].",
                e
            );
        }
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        innerToXContent(builder);
        builder.endObject();
        return builder;
    }

    public void innerToXContent(XContentBuilder builder) throws IOException {
        builder.array(SEARCH_AFTER.getPreferredName(), sortValues);
    }

    public static SearchAfterBuilder fromXContent(XContentParser parser) throws IOException {
        SearchAfterBuilder builder = new SearchAfterBuilder();
        XContentParser.Token token = parser.currentToken();
        List<Object> values = new ArrayList<>();
        if (token == XContentParser.Token.START_ARRAY) {
            while ((token = parser.nextToken()) != XContentParser.Token.END_ARRAY) {
                if (token == XContentParser.Token.VALUE_NUMBER) {
                    switch (parser.numberType()) {
                        case INT -> values.add(parser.intValue());
                        case LONG -> values.add(parser.longValue());
                        case DOUBLE -> values.add(parser.doubleValue());
                        case FLOAT -> values.add(parser.floatValue());
                        case BIG_INTEGER -> values.add(parser.text());
                        default -> throw new IllegalArgumentException(
                            "[search_after] does not accept numbers of type [" + parser.numberType() + "], got " + parser.text()
                        );
                    }
                } else if (token == XContentParser.Token.VALUE_STRING) {
                    values.add(parser.text());
                } else if (token == XContentParser.Token.VALUE_BOOLEAN) {
                    values.add(parser.booleanValue());
                } else if (token == XContentParser.Token.VALUE_NULL) {
                    values.add(null);
                } else {
                    throw new ParsingException(
                        parser.getTokenLocation(),
                        "Expected ["
                            + XContentParser.Token.VALUE_STRING
                            + "] or ["
                            + XContentParser.Token.VALUE_NUMBER
                            + "] or ["
                            + XContentParser.Token.VALUE_BOOLEAN
                            + "] or ["
                            + XContentParser.Token.VALUE_NULL
                            + "] but found ["
                            + token
                            + "] inside search_after."
                    );
                }
            }
        } else {
            throw new ParsingException(
                parser.getTokenLocation(),
                "Expected ["
                    + XContentParser.Token.START_ARRAY
                    + "] in ["
                    + SEARCH_AFTER.getPreferredName()
                    + "] but found ["
                    + token
                    + "] inside search_after",
                parser.getTokenLocation()
            );
        }
        builder.setSortValues(values.toArray());
        return builder;
    }

    @Override
    public boolean equals(Object other) {
        if ((other instanceof SearchAfterBuilder) == false) {
            return false;
        }
        boolean value = Arrays.equals(sortValues, ((SearchAfterBuilder) other).sortValues);
        return value;
    }

    @Override
    public int hashCode() {
        return Objects.hash(this.sortValues);
    }

    @Override
    public String toString() {
        return Strings.toString(this, true, true);
    }
}
