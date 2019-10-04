/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.elasticsearch.search.searchafter;

import org.apache.lucene.search.FieldDoc;
import org.apache.lucene.search.SortField;
import org.apache.lucene.search.SortedNumericSortField;
import org.apache.lucene.search.SortedSetSortField;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.common.ParseField;
import org.elasticsearch.common.ParsingException;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.text.Text;
import org.elasticsearch.common.xcontent.ToXContentObject;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.index.fielddata.IndexFieldData;
import org.elasticsearch.search.DocValueFormat;
import org.elasticsearch.search.sort.SortAndFormats;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;

public class SearchAfterBuilder implements ToXContentObject, Writeable {
    public static final ParseField SEARCH_AFTER = new ParseField("search_after");
    private static final Object[] EMPTY_SORT_VALUES = new Object[0];

    private Object[] sortValues = EMPTY_SORT_VALUES;

    public SearchAfterBuilder() {
    }

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
        out.writeVInt(sortValues.length);
        for (Object fieldValue : sortValues) {
            out.writeGenericValue(fieldValue);
        }
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
            throw new IllegalArgumentException("Can't handle " + SEARCH_AFTER + " field value of type [" + values[i].getClass() + "]");
        }
        sortValues = new Object[values.length];
        System.arraycopy(values, 0, sortValues, 0, values.length);
        return this;
    }

    public Object[] getSortValues() {
        return Arrays.copyOf(sortValues, sortValues.length);
    }

    public static FieldDoc buildFieldDoc(SortAndFormats sort, Object[] values) {
        if (sort == null || sort.sort.getSort() == null || sort.sort.getSort().length == 0) {
            throw new IllegalArgumentException("Sort must contain at least one field.");
        }

        SortField[] sortFields = sort.sort.getSort();
        if (sortFields.length != values.length) {
            throw new IllegalArgumentException(
                    SEARCH_AFTER.getPreferredName() + " has " + values.length + " value(s) but sort has "
                            + sort.sort.getSort().length + ".");
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
                case DOC:
                    if (value instanceof Number) {
                        return ((Number) value).intValue();
                    }
                    return Integer.parseInt(value.toString());

                case SCORE:
                    if (value instanceof Number) {
                        return ((Number) value).floatValue();
                    }
                    return Float.parseFloat(value.toString());

                case INT:
                    if (value instanceof Number) {
                        return ((Number) value).intValue();
                    }
                    return Integer.parseInt(value.toString());

                case DOUBLE:
                    if (value instanceof Number) {
                        return ((Number) value).doubleValue();
                    }
                    return Double.parseDouble(value.toString());

                case LONG:
                    if (value instanceof Number) {
                        return ((Number) value).longValue();
                    }
                    return Long.parseLong(value.toString());

                case FLOAT:
                    if (value instanceof Number) {
                        return ((Number) value).floatValue();
                    }
                    return Float.parseFloat(value.toString());

                case STRING_VAL:
                case STRING:
                    return format.parseBytesRef(value.toString());

                default:
                    throw new IllegalArgumentException("Comparator type [" + sortType.name() + "] for field [" + fieldName
                            + "] is not supported.");
            }
        } catch(NumberFormatException e) {
            throw new IllegalArgumentException(
                    "Failed to parse " + SEARCH_AFTER.getPreferredName() + " value for field [" + fieldName + "].", e);
        }
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        innerToXContent(builder);
        builder.endObject();
        return builder;
    }

    void innerToXContent(XContentBuilder builder) throws IOException {
        builder.array(SEARCH_AFTER.getPreferredName(), sortValues);
    }

    public static SearchAfterBuilder fromXContent(XContentParser parser) throws IOException {
        SearchAfterBuilder builder = new SearchAfterBuilder();
        XContentParser.Token token = parser.currentToken();
        List<Object> values = new ArrayList<> ();
        if (token == XContentParser.Token.START_ARRAY) {
            while ((token = parser.nextToken()) != XContentParser.Token.END_ARRAY) {
                if (token == XContentParser.Token.VALUE_NUMBER) {
                    switch (parser.numberType()) {
                        case INT:
                            values.add(parser.intValue());
                            break;

                        case LONG:
                            values.add(parser.longValue());
                            break;

                        case DOUBLE:
                            values.add(parser.doubleValue());
                            break;

                        case FLOAT:
                            values.add(parser.floatValue());
                            break;

                        default:
                            throw new AssertionError("Unknown number type []" + parser.numberType());
                    }
                } else if (token == XContentParser.Token.VALUE_STRING) {
                    values.add(parser.text());
                } else if (token == XContentParser.Token.VALUE_BOOLEAN) {
                    values.add(parser.booleanValue());
                } else if (token == XContentParser.Token.VALUE_NULL) {
                    values.add(null);
                } else {
                    throw new ParsingException(parser.getTokenLocation(), "Expected [" + XContentParser.Token.VALUE_STRING + "] or ["
                            + XContentParser.Token.VALUE_NUMBER + "] or [" + XContentParser.Token.VALUE_BOOLEAN + "] or ["
                            + XContentParser.Token.VALUE_NULL + "] but found [" + token + "] inside search_after.");
                }
            }
        } else {
            throw new ParsingException(parser.getTokenLocation(), "Expected [" + XContentParser.Token.START_ARRAY + "] in ["
                    + SEARCH_AFTER.getPreferredName() + "] but found [" + token + "] inside search_after", parser.getTokenLocation());
        }
        builder.setSortValues(values.toArray());
        return builder;
    }

    @Override
    public boolean equals(Object other) {
        if (! (other instanceof SearchAfterBuilder)) {
            return false;
        }
        return Arrays.equals(sortValues, ((SearchAfterBuilder) other).sortValues);
    }

    @Override
    public int hashCode() {
        return Objects.hash(this.sortValues);
    }

    @Override
    public String toString() {
        try {
            XContentBuilder builder = XContentFactory.jsonBuilder();
            builder.prettyPrint();
            toXContent(builder, EMPTY_PARAMS);
            return Strings.toString(builder);
        } catch (Exception e) {
            throw new ElasticsearchException("Failed to build xcontent.", e);
        }
    }
}
