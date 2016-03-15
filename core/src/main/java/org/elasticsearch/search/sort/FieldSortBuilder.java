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

package org.elasticsearch.search.sort;

import org.apache.lucene.search.SortField;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.common.ParseField;
import org.elasticsearch.common.ParsingException;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.lucene.BytesRefs;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.index.fielddata.IndexFieldData;
import org.elasticsearch.index.fielddata.IndexFieldData.XFieldComparatorSource.Nested;
import org.elasticsearch.index.mapper.MappedFieldType;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryParseContext;
import org.elasticsearch.index.query.QueryShardContext;
import org.elasticsearch.index.query.QueryShardException;
import org.elasticsearch.search.MultiValueMode;

import java.io.IOException;
import java.util.Objects;

/**
 * A sort builder to sort based on a document field.
 */
public class FieldSortBuilder extends SortBuilder<FieldSortBuilder> implements SortBuilderParser<FieldSortBuilder> {
    static final FieldSortBuilder PROTOTYPE = new FieldSortBuilder("");
    public static final String NAME = "field_sort";
    public static final ParseField NESTED_PATH = new ParseField("nested_path");
    public static final ParseField NESTED_FILTER = new ParseField("nested_filter");
    public static final ParseField MISSING = new ParseField("missing");
    public static final ParseField ORDER = new ParseField("order");
    public static final ParseField REVERSE = new ParseField("reverse");
    public static final ParseField SORT_MODE = new ParseField("mode");
    public static final ParseField UNMAPPED_TYPE = new ParseField("unmapped_type");

    /**
     * special field name to sort by index order
     */
    public static final String DOC_FIELD_NAME = "_doc";
    private static final SortField SORT_DOC = new SortField(null, SortField.Type.DOC);
    private static final SortField SORT_DOC_REVERSE = new SortField(null, SortField.Type.DOC, true);

    private final String fieldName;

    private Object missing;

    private String unmappedType;

    private SortMode sortMode;

    private QueryBuilder<?> nestedFilter;

    private String nestedPath;

    /** Copy constructor. */
    public FieldSortBuilder(FieldSortBuilder template) {
        this(template.fieldName);
        this.order(template.order());
        this.missing(template.missing());
        this.unmappedType(template.unmappedType());
        if (template.sortMode != null) {
            this.sortMode(template.sortMode());
        }
        this.setNestedFilter(template.getNestedFilter());
        this.setNestedPath(template.getNestedPath());
    }

    /**
     * Constructs a new sort based on a document field.
     *
     * @param fieldName
     *            The field name.
     */
    public FieldSortBuilder(String fieldName) {
        if (fieldName == null) {
            throw new IllegalArgumentException("fieldName must not be null");
        }
        this.fieldName = fieldName;
    }

    /** Returns the document field this sort should be based on. */
    public String getFieldName() {
        return this.fieldName;
    }

    /**
     * Sets the value when a field is missing in a doc. Can also be set to <tt>_last</tt> or
     * <tt>_first</tt> to sort missing last or first respectively.
     */
    public FieldSortBuilder missing(Object missing) {
        if (missing instanceof String) {
            this.missing = BytesRefs.toBytesRef(missing);
        } else {
            this.missing = missing;
        }
        return this;
    }

    /** Returns the value used when a field is missing in a doc. */
    public Object missing() {
        if (missing instanceof BytesRef) {
            return ((BytesRef) missing).utf8ToString();
        }
        return missing;
    }

    /**
     * Set the type to use in case the current field is not mapped in an index.
     * Specifying a type tells Elasticsearch what type the sort values should
     * have, which is important for cross-index search, if there are sort fields
     * that exist on some indices only. If the unmapped type is <tt>null</tt>
     * then query execution will fail if one or more indices don't have a
     * mapping for the current field.
     */
    public FieldSortBuilder unmappedType(String type) {
        this.unmappedType = type;
        return this;
    }

    /**
     * Returns the type to use in case the current field is not mapped in an
     * index.
     */
    public String unmappedType() {
        return this.unmappedType;
    }

    /**
     * Defines what values to pick in the case a document contains multiple
     * values for the targeted sort field. Possible values: min, max, sum and
     * avg
     *
     * <p>
     * The last two values are only applicable for number based fields.
     */
    public FieldSortBuilder sortMode(SortMode sortMode) {
        Objects.requireNonNull(sortMode, "sort mode cannot be null");
        this.sortMode = sortMode;
        return this;
    }

    /**
     * Returns what values to pick in the case a document contains multiple
     * values for the targeted sort field.
     */
    public SortMode sortMode() {
        return this.sortMode;
    }

    /**
     * Sets the nested filter that the nested objects should match with in order
     * to be taken into account for sorting.
     *
     * TODO should the above getters and setters be deprecated/ changed in
     * favour of real getters and setters?
     */
    public FieldSortBuilder setNestedFilter(QueryBuilder<?> nestedFilter) {
        this.nestedFilter = nestedFilter;
        return this;
    }

    /**
     * Returns the nested filter that the nested objects should match with in
     * order to be taken into account for sorting.
     */
    public QueryBuilder<?> getNestedFilter() {
        return this.nestedFilter;
    }

    /**
     * Sets the nested path if sorting occurs on a field that is inside a nested
     * object. By default when sorting on a field inside a nested object, the
     * nearest upper nested object is selected as nested path.
     */
    public FieldSortBuilder setNestedPath(String nestedPath) {
        this.nestedPath = nestedPath;
        return this;
    }

    /**
     * Returns the nested path if sorting occurs in a field that is inside a
     * nested object.
     */
    public String getNestedPath() {
        return this.nestedPath;
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject(fieldName);
        builder.field(ORDER_FIELD.getPreferredName(), order);
        if (missing != null) {
            if (missing instanceof BytesRef) {
                builder.field(MISSING.getPreferredName(), ((BytesRef) missing).utf8ToString());
            } else {
                builder.field(MISSING.getPreferredName(), missing);
            }
        }
        if (unmappedType != null) {
            builder.field(UNMAPPED_TYPE.getPreferredName(), unmappedType);
        }
        if (sortMode != null) {
            builder.field(SORT_MODE.getPreferredName(), sortMode);
        }
        if (nestedFilter != null) {
            builder.field(NESTED_FILTER.getPreferredName(), nestedFilter, params);
        }
        if (nestedPath != null) {
            builder.field(NESTED_PATH.getPreferredName(), nestedPath);
        }
        builder.endObject();
        return builder;
    }

    @Override
    public SortField build(QueryShardContext context) throws IOException {
        if (DOC_FIELD_NAME.equals(fieldName)) {
            if (order == SortOrder.DESC) {
                return SORT_DOC_REVERSE;
            } else {
                return SORT_DOC;
            }
        } else {
            MappedFieldType fieldType = context.fieldMapper(fieldName);
            if (fieldType == null) {
                if (unmappedType != null) {
                    fieldType = context.getMapperService().unmappedFieldType(unmappedType);
                } else {
                    throw new QueryShardException(context, "No mapping found for [" + fieldName + "] in order to sort on");
                }
            }

            if (!fieldType.isSortable()) {
                throw new QueryShardException(context, "Sorting not supported for field[" + fieldName + "]");
            }

            // Enable when we also know how to detect fields that do tokenize, but only emit one token
            /*if (fieldMapper instanceof StringFieldMapper) {
                StringFieldMapper stringFieldMapper = (StringFieldMapper) fieldMapper;
                if (stringFieldMapper.fieldType().tokenized()) {
                    // Fail early
                    throw new SearchParseException(context, "Can't sort on tokenized string field[" + fieldName + "]");
                }
            }*/

            MultiValueMode localSortMode = null;
            if (sortMode != null) {
                localSortMode = MultiValueMode.fromString(sortMode.toString());
            }
            if (fieldType.isNumeric() == false && (sortMode == SortMode.SUM || sortMode == SortMode.AVG)) {
                throw new QueryShardException(context, "we only support AVG and SUM on number based fields");
            }
            boolean reverse = (order == SortOrder.DESC);
            if (localSortMode == null) {
                localSortMode = reverse ? MultiValueMode.MAX : MultiValueMode.MIN;
            }

            final Nested nested = resolveNested(context, nestedPath, nestedFilter);
            IndexFieldData.XFieldComparatorSource fieldComparatorSource = context.getForField(fieldType)
                    .comparatorSource(missing, localSortMode, nested);
            return new SortField(fieldType.name(), fieldComparatorSource, reverse);
        }
    }

    @Override
    public boolean equals(Object other) {
        if (this == other) {
            return true;
        }

        if (other == null || getClass() != other.getClass()) {
            return false;
        }

        FieldSortBuilder builder = (FieldSortBuilder) other;
        return (Objects.equals(this.fieldName, builder.fieldName) && Objects.equals(this.nestedFilter, builder.nestedFilter)
                && Objects.equals(this.nestedPath, builder.nestedPath) && Objects.equals(this.missing, builder.missing)
                && Objects.equals(this.order, builder.order) && Objects.equals(this.sortMode, builder.sortMode)
                && Objects.equals(this.unmappedType, builder.unmappedType));
    }

    @Override
    public int hashCode() {
        return Objects.hash(this.fieldName, this.nestedFilter, this.nestedPath, this.missing, this.order, this.sortMode, this.unmappedType);
    }

    @Override
    public String getWriteableName() {
        return NAME;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeString(this.fieldName);
        if (this.nestedFilter != null) {
            out.writeBoolean(true);
            out.writeQuery(this.nestedFilter);
        } else {
            out.writeBoolean(false);
        }
        out.writeOptionalString(this.nestedPath);
        out.writeGenericValue(this.missing);

        if (this.order != null) {
            out.writeBoolean(true);
            this.order.writeTo(out);
        } else {
            out.writeBoolean(false);
        }

        out.writeBoolean(this.sortMode != null);
        if (this.sortMode != null) {
           this.sortMode.writeTo(out);
        }
        out.writeOptionalString(this.unmappedType);
    }

    @Override
    public FieldSortBuilder readFrom(StreamInput in) throws IOException {
        String fieldName = in.readString();
        FieldSortBuilder result = new FieldSortBuilder(fieldName);
        if (in.readBoolean()) {
            QueryBuilder<?> query = in.readQuery();
            result.setNestedFilter(query);
        }
        result.setNestedPath(in.readOptionalString());
        result.missing(in.readGenericValue());

        if (in.readBoolean()) {
            result.order(SortOrder.readOrderFrom(in));
        }
        if (in.readBoolean()) {
            result.sortMode(SortMode.PROTOTYPE.readFrom(in));
        }
        result.unmappedType(in.readOptionalString());
        return result;
    }

    @Override
    public FieldSortBuilder fromXContent(QueryParseContext context, String fieldName) throws IOException {
        XContentParser parser = context.parser();

        QueryBuilder<?> nestedFilter = null;
        String nestedPath = null;
        Object missing = null;
        SortOrder order = null;
        SortMode sortMode = null;
        String unmappedType = null;

        String currentFieldName = null;
        XContentParser.Token token;
        while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
            if (token == XContentParser.Token.FIELD_NAME) {
                currentFieldName = parser.currentName();
            } else if (token == XContentParser.Token.START_OBJECT) {
                if (context.parseFieldMatcher().match(currentFieldName, NESTED_FILTER)) {
                    nestedFilter = context.parseInnerQueryBuilder();
                } else {
                    throw new ParsingException(parser.getTokenLocation(), "Expected " + NESTED_FILTER.getPreferredName() + " element.");
                }
            } else if (token.isValue()) {
                if (context.parseFieldMatcher().match(currentFieldName, NESTED_PATH)) {
                    nestedPath = parser.text();
                } else if (context.parseFieldMatcher().match(currentFieldName, MISSING)) {
                    missing = parser.objectBytes();
                } else if (context.parseFieldMatcher().match(currentFieldName, REVERSE)) {
                    if (parser.booleanValue()) {
                        order = SortOrder.DESC;
                    }
                    // else we keep the default ASC
                } else if (context.parseFieldMatcher().match(currentFieldName, ORDER)) {
                    String sortOrder = parser.text();
                    if ("asc".equals(sortOrder)) {
                        order = SortOrder.ASC;
                    } else if ("desc".equals(sortOrder)) {
                        order = SortOrder.DESC;
                    } else {
                        throw new IllegalStateException("Sort order " + sortOrder + " not supported.");
                    }
                } else if (context.parseFieldMatcher().match(currentFieldName, SORT_MODE)) {
                    sortMode = SortMode.fromString(parser.text());
                } else if (context.parseFieldMatcher().match(currentFieldName, UNMAPPED_TYPE)) {
                    unmappedType = parser.text();
                } else {
                    throw new IllegalArgumentException("Option " + currentFieldName + " not supported.");
                }
            }
        }

        FieldSortBuilder builder = new FieldSortBuilder(fieldName);
        if (nestedFilter != null) {
            builder.setNestedFilter(nestedFilter);
        }
        if (nestedPath != null) {
            builder.setNestedPath(nestedPath);
        }
        if (missing != null) {
            builder.missing(missing);
        }
        if (order != null) {
            builder.order(order);
        }
        if (sortMode != null) {
            builder.sortMode(sortMode);
        }
        if (unmappedType != null) {
            builder.unmappedType(unmappedType);
        }
        return builder;
    }
}
