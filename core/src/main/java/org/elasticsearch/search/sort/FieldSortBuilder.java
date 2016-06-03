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
import org.elasticsearch.common.ParseField;
import org.elasticsearch.common.ParsingException;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.index.fielddata.IndexFieldData;
import org.elasticsearch.index.fielddata.IndexFieldData.XFieldComparatorSource.Nested;
import org.elasticsearch.index.fielddata.IndexNumericFieldData;
import org.elasticsearch.index.mapper.MappedFieldType;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryParseContext;
import org.elasticsearch.index.query.QueryShardContext;
import org.elasticsearch.index.query.QueryShardException;
import org.elasticsearch.search.DocValueFormat;
import org.elasticsearch.search.MultiValueMode;

import java.io.IOException;
import java.util.Objects;
import java.util.Optional;

/**
 * A sort builder to sort based on a document field.
 */
public class FieldSortBuilder extends SortBuilder<FieldSortBuilder> {
    public static final String NAME = "field_sort";
    public static final ParseField NESTED_PATH = new ParseField("nested_path");
    public static final ParseField NESTED_FILTER = new ParseField("nested_filter");
    public static final ParseField MISSING = new ParseField("missing");
    public static final ParseField ORDER = new ParseField("order");
    public static final ParseField SORT_MODE = new ParseField("mode");
    public static final ParseField UNMAPPED_TYPE = new ParseField("unmapped_type");

    /**
     * special field name to sort by index order
     */
    public static final String DOC_FIELD_NAME = "_doc";
    private static final SortFieldAndFormat SORT_DOC = new SortFieldAndFormat(
            new SortField(null, SortField.Type.DOC), DocValueFormat.RAW);
    private static final SortFieldAndFormat SORT_DOC_REVERSE = new SortFieldAndFormat(
            new SortField(null, SortField.Type.DOC, true), DocValueFormat.RAW);

    private final String fieldName;

    private Object missing;

    private String unmappedType;

    private SortMode sortMode;

    private QueryBuilder nestedFilter;

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

    /**
     * Read from a stream.
     */
    public FieldSortBuilder(StreamInput in) throws IOException {
        fieldName = in.readString();
        nestedFilter = in.readOptionalNamedWriteable(QueryBuilder.class);
        nestedPath = in.readOptionalString();
        missing = in.readGenericValue();
        order = in.readOptionalWriteable(SortOrder::readFromStream);
        sortMode = in.readOptionalWriteable(SortMode::readFromStream);
        unmappedType = in.readOptionalString();
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeString(fieldName);
        out.writeOptionalNamedWriteable(nestedFilter);
        out.writeOptionalString(nestedPath);
        out.writeGenericValue(missing);
        out.writeOptionalWriteable(order);
        out.writeOptionalWriteable(sortMode);
        out.writeOptionalString(unmappedType);
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
        this.missing = missing;
        return this;
    }

    /** Returns the value used when a field is missing in a doc. */
    public Object missing() {
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
    public FieldSortBuilder setNestedFilter(QueryBuilder nestedFilter) {
        this.nestedFilter = nestedFilter;
        return this;
    }

    /**
     * Returns the nested filter that the nested objects should match with in
     * order to be taken into account for sorting.
     */
    public QueryBuilder getNestedFilter() {
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
        builder.startObject();
        builder.startObject(fieldName);
        builder.field(ORDER_FIELD.getPreferredName(), order);
        if (missing != null) {
            builder.field(MISSING.getPreferredName(), missing);
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
        builder.endObject();
        return builder;
    }

    @Override
    public SortFieldAndFormat build(QueryShardContext context) throws IOException {
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

            MultiValueMode localSortMode = null;
            if (sortMode != null) {
                localSortMode = MultiValueMode.fromString(sortMode.toString());
            }

            boolean reverse = (order == SortOrder.DESC);
            if (localSortMode == null) {
                localSortMode = reverse ? MultiValueMode.MAX : MultiValueMode.MIN;
            }

            final Nested nested = resolveNested(context, nestedPath, nestedFilter);
            IndexFieldData<?> fieldData = context.getForField(fieldType);
            if (fieldData instanceof IndexNumericFieldData == false
                    && (sortMode == SortMode.SUM || sortMode == SortMode.AVG || sortMode == SortMode.MEDIAN)) {
                throw new QueryShardException(context, "we only support AVG, MEDIAN and SUM on number based fields");
            }
            IndexFieldData.XFieldComparatorSource fieldComparatorSource = fieldData
                    .comparatorSource(missing, localSortMode, nested);
            SortField field = new SortField(fieldType.name(), fieldComparatorSource, reverse);
            return new SortFieldAndFormat(field, fieldType.docValueFormat(null, null));
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

    /**
     * Creates a new {@link FieldSortBuilder} from the query held by the {@link QueryParseContext} in
     * {@link org.elasticsearch.common.xcontent.XContent} format.
     *
     * @param context the input parse context. The state on the parser contained in this context will be changed as a side effect of this
     *        method call
     * @param fieldName in some sort syntax variations the field name precedes the xContent object that specifies further parameters, e.g.
     *        in '{ "foo": { "order" : "asc"} }'. When parsing the inner object, the field name can be passed in via this argument
     */
    public static FieldSortBuilder fromXContent(QueryParseContext context, String fieldName) throws IOException {
        XContentParser parser = context.parser();

        Optional<QueryBuilder> nestedFilter = Optional.empty();
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
                if (context.getParseFieldMatcher().match(currentFieldName, NESTED_FILTER)) {
                    nestedFilter = context.parseInnerQueryBuilder();
                } else {
                    throw new ParsingException(parser.getTokenLocation(), "Expected " + NESTED_FILTER.getPreferredName() + " element.");
                }
            } else if (token.isValue()) {
                if (context.getParseFieldMatcher().match(currentFieldName, NESTED_PATH)) {
                    nestedPath = parser.text();
                } else if (context.getParseFieldMatcher().match(currentFieldName, MISSING)) {
                    missing = parser.objectText();
                } else if (context.getParseFieldMatcher().match(currentFieldName, ORDER)) {
                    String sortOrder = parser.text();
                    if ("asc".equals(sortOrder)) {
                        order = SortOrder.ASC;
                    } else if ("desc".equals(sortOrder)) {
                        order = SortOrder.DESC;
                    } else {
                        throw new ParsingException(parser.getTokenLocation(), "Sort order [{}] not supported.", sortOrder);
                    }
                } else if (context.getParseFieldMatcher().match(currentFieldName, SORT_MODE)) {
                    sortMode = SortMode.fromString(parser.text());
                } else if (context.getParseFieldMatcher().match(currentFieldName, UNMAPPED_TYPE)) {
                    unmappedType = parser.text();
                } else {
                    throw new ParsingException(parser.getTokenLocation(), "Option [{}] not supported.", currentFieldName);
                }
            }
        }

        FieldSortBuilder builder = new FieldSortBuilder(fieldName);
        nestedFilter.ifPresent(builder::setNestedFilter);
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
