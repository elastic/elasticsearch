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
import org.apache.lucene.search.Sort;
import org.apache.lucene.search.SortField;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.common.ParseField;
import org.elasticsearch.common.ParseFieldMatcher;
import org.elasticsearch.common.ParsingException;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.text.Text;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.FromXContentBuilder;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.index.fielddata.IndexFieldData;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;

/**
 *
 */
public class SearchAfterBuilder implements ToXContent, FromXContentBuilder<SearchAfterBuilder>, Writeable<SearchAfterBuilder> {
    public static final SearchAfterBuilder PROTOTYPE = new SearchAfterBuilder();
    public static final ParseField SEARCH_AFTER = new ParseField("search_after");
    private static final Object[] EMPTY_SORT_VALUES = new Object[0];

    private Object[] sortValues = EMPTY_SORT_VALUES;

    public SearchAfterBuilder setSortValues(Object[] values) {
        if (values == null) {
            throw new NullPointerException("Values cannot be null.");
        }
        if (values.length == 0) {
            throw new IllegalArgumentException("Values must contains at least one value.");
        }
        sortValues = new Object[values.length];
        System.arraycopy(values, 0, sortValues, 0, values.length);
        return this;
    }

    public Object[] getSortValues() {
        return sortValues;
    }

    public static FieldDoc buildFieldDoc(Sort sort, Object[] values) {
        if (sort == null || sort.getSort() == null || sort.getSort().length == 0) {
            throw new IllegalArgumentException("Sort must contain at least one field.");
        }

        SortField[] sortFields = sort.getSort();
        if (sortFields.length != values.length) {
            throw new IllegalArgumentException(SEARCH_AFTER.getPreferredName() + " has " + values.length + " value(s) but sort has " + sort.getSort().length + ".");
        }
        Object[] fieldValues = new Object[sortFields.length];
        for (int i = 0; i < sortFields.length; i++) {
            SortField sortField = sortFields[i];
            if (values[i] != null) {
                fieldValues[i] = convertValueFromSortField(values[i], sortField);
            } else {
                fieldValues[i] = null;
            }
        }
        // We set the doc id to Integer.MAX_VALUE in order to make sure that the search starts "after" the first document that is equal to the field values.
        return new FieldDoc(Integer.MAX_VALUE, 0, fieldValues);
    }

    private static Object convertValueFromSortField(Object value, SortField sortField) {
        if (sortField.getComparatorSource() instanceof IndexFieldData.XFieldComparatorSource) {
            IndexFieldData.XFieldComparatorSource cmpSource = (IndexFieldData.XFieldComparatorSource) sortField.getComparatorSource();
            return convertValueFromSortType(sortField.getField(), cmpSource.reducedType(), value);
        }
        return convertValueFromSortType(sortField.getField(), sortField.getType(), value);
    }

    private static Object convertValueFromSortType(String fieldName, SortField.Type sortType, Object value) {
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
                    return new BytesRef(value.toString());

                default:
                    throw new IllegalArgumentException("Comparator type [" + sortType.name() + "] for field [" + fieldName + "] is not supported.");
            }
        } catch(NumberFormatException e) {
            throw new IllegalArgumentException("Failed to parse " + SEARCH_AFTER.getPreferredName() + " value for field [" + fieldName + "].", e);
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
        builder.field(SEARCH_AFTER.getPreferredName(), sortValues);
    }

    @Override
    public SearchAfterBuilder fromXContent(XContentParser parser, ParseFieldMatcher parseFieldMatcher) throws IOException {
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
                    throw new ParsingException(parser.getTokenLocation(), "Expected [" + XContentParser.Token.VALUE_STRING + "] or [" + XContentParser.Token.VALUE_NUMBER + "] or [" + XContentParser.Token.VALUE_BOOLEAN + "] or [" + XContentParser.Token.VALUE_NULL + "] but found [" + token + "] inside search_after.", parser.getTokenLocation());
                }
            }
        } else {
            throw new ParsingException(parser.getTokenLocation(), "Expected [" + XContentParser.Token.START_ARRAY + "] in [" + SEARCH_AFTER.getPreferredName() + "] but found [" + token + "] inside search_after", parser.getTokenLocation());
        }
        builder.setSortValues(values.toArray());
        return builder;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeVInt(sortValues.length);
        for (Object fieldValue : sortValues) {
            if (fieldValue == null) {
                out.writeByte((byte) 0);
            } else {
                Class<?> type = fieldValue.getClass();
                if (type == String.class) {
                    out.writeByte((byte) 1);
                    out.writeString((String) fieldValue);
                } else if (type == Integer.class) {
                    out.writeByte((byte) 2);
                    out.writeInt((Integer) fieldValue);
                } else if (type == Long.class) {
                    out.writeByte((byte) 3);
                    out.writeLong((Long) fieldValue);
                } else if (type == Float.class) {
                    out.writeByte((byte) 4);
                    out.writeFloat((Float) fieldValue);
                } else if (type == Double.class) {
                    out.writeByte((byte) 5);
                    out.writeDouble((Double) fieldValue);
                } else if (type == Byte.class) {
                    out.writeByte((byte) 6);
                    out.writeByte((Byte) fieldValue);
                } else if (type == Short.class) {
                    out.writeByte((byte) 7);
                    out.writeShort((Short) fieldValue);
                } else if (type == Boolean.class) {
                    out.writeByte((byte) 8);
                    out.writeBoolean((Boolean) fieldValue);
                } else if (fieldValue instanceof Text) {
                    out.writeByte((byte) 9);
                    out.writeText((Text) fieldValue);
                } else {
                    throw new IOException("Can't handle " + SEARCH_AFTER.getPreferredName() + " field value of type [" + type + "]");
                }
            }
        }
    }

    @Override
    public SearchAfterBuilder readFrom(StreamInput in) throws IOException {
        SearchAfterBuilder builder = new SearchAfterBuilder();
        int size = in.readVInt();
        Object[] values = new Object[size];
        for (int i = 0; i < size; i++) {
            byte type = in.readByte();
            if (type == 0) {
                values[i] = null;
            } else if (type == 1) {
                values[i] = in.readString();
            } else if (type == 2) {
                values[i] = in.readInt();
            } else if (type == 3) {
                values[i] = in.readLong();
            } else if (type == 4) {
                values[i] = in.readFloat();
            } else if (type == 5) {
                values[i] = in.readDouble();
            } else if (type == 6) {
                values[i] = in.readByte();
            } else if (type == 7) {
                values[i] = in.readShort();
            } else if (type == 8) {
                values[i] = in.readBoolean();
            } else if (type == 9) {
                values[i] = in.readText();
            } else {
                throw new IOException("Can't match type [" + type + "]");
            }
        }
        builder.setSortValues(values);
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
            return builder.string();
        } catch (Exception e) {
            throw new ElasticsearchException("Failed to build xcontent.", e);
        }
    }
}
