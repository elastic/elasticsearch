/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.query.bitmapterms;

import org.apache.lucene.search.Query;
import org.elasticsearch.TransportVersion;
import org.elasticsearch.common.ParsingException;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.index.mapper.MappedFieldType;
import org.elasticsearch.index.mapper.NumberFieldMapper;
import org.elasticsearch.index.query.AbstractQueryBuilder;
import org.elasticsearch.index.query.SearchExecutionContext;
import org.elasticsearch.search.internal.MaxClauseCountQueryVisitor;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentParser;
import org.roaringbitmap.RoaringBitmap;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.Base64;
import java.util.Objects;

/**
 * A query that matches documents whose integer field value is present in a RoaringBitmap,
 * supplied as a base64-encoded serialized bitmap in little-endian byte order.
 * <p>
 * Only supported on indexed {@code integer} fields ({@code index: true}).
 * Only non-negative values (0 to 2,147,483,647) are supported.
 * <p>
 * Example:
 * <pre>{@code
 * {
 *   "bitmap_terms": {
 *     "field": "my_integer_field",
 *     "value": "<base64-encoded bitmap>"
 *   }
 * }
 * }</pre>
 */
public class BitmapTermsQueryBuilder extends AbstractQueryBuilder<BitmapTermsQueryBuilder> {

    public static final String NAME = "bitmap_terms";

    static final TransportVersion BITMAP_TERMS_QUERY_ADDED = TransportVersion.fromName("bitmap_terms_query_added");

    private final String fieldName;
    private final String value;

    public BitmapTermsQueryBuilder(String fieldName, String value) {
        if (fieldName == null || fieldName.isBlank()) {
            throw new IllegalArgumentException("[bitmap_terms] field name cannot be null or empty");
        }
        if (value == null) {
            throw new IllegalArgumentException("[bitmap_terms] value cannot be null");
        }
        this.fieldName = fieldName;
        this.value = value;
    }

    public BitmapTermsQueryBuilder(StreamInput in) throws IOException {
        super(in);
        this.fieldName = in.readString();
        this.value = in.readString();
    }

    @Override
    protected void doWriteTo(StreamOutput out) throws IOException {
        out.writeString(fieldName);
        out.writeString(value);
    }

    public String fieldName() {
        return fieldName;
    }

    public String value() {
        return value;
    }

    @Override
    protected void doXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject(NAME);
        builder.field("field", fieldName);
        builder.field("value", value);
        printBoostAndQueryName(builder);
        builder.endObject();
    }

    public static BitmapTermsQueryBuilder fromXContent(XContentParser parser) throws IOException {
        String fieldName = null;
        String value = null;
        float boost = DEFAULT_BOOST;
        String queryName = null;

        XContentParser.Token token;
        String currentFieldName = null;
        while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
            if (token == XContentParser.Token.FIELD_NAME) {
                currentFieldName = parser.currentName();
            } else if (token.isValue()) {
                if ("field".equals(currentFieldName)) {
                    fieldName = parser.text();
                } else if ("value".equals(currentFieldName)) {
                    value = parser.text();
                } else if (BOOST_FIELD.match(currentFieldName, parser.getDeprecationHandler())) {
                    boost = parser.floatValue();
                } else if (NAME_FIELD.match(currentFieldName, parser.getDeprecationHandler())) {
                    queryName = parser.text();
                } else {
                    throw new ParsingException(
                        parser.getTokenLocation(),
                        "[" + NAME + "] query does not support [" + currentFieldName + "]"
                    );
                }
            } else {
                throw new ParsingException(
                    parser.getTokenLocation(),
                    "[" + NAME + "] unknown token [" + token + "] after [" + currentFieldName + "]"
                );
            }
        }
        if (fieldName == null) {
            throw new ParsingException(parser.getTokenLocation(), "[" + NAME + "] requires a [field]");
        }
        if (value == null) {
            throw new ParsingException(parser.getTokenLocation(), "[" + NAME + "] requires a [value]");
        }
        return new BitmapTermsQueryBuilder(fieldName, value).boost(boost).queryName(queryName);
    }

    @Override
    public String getWriteableName() {
        return NAME;
    }

    @Override
    protected Query doToQuery(SearchExecutionContext context, MaxClauseCountQueryVisitor visitor) throws IOException {
        MappedFieldType fieldType = context.getFieldType(fieldName);
        if (!(fieldType instanceof NumberFieldMapper.NumberFieldType numberFieldType)
            || numberFieldType.numberType() != NumberFieldMapper.NumberType.INTEGER
            || (numberFieldType.isIndexedWithPoints() == false && numberFieldType.isIndexedWithTerms() == false)) {
            throw new IllegalArgumentException(
                "[bitmap_terms] query is not supported on field ["
                    + fieldName
                    + "]: only supported on [integer] fields indexed with points or terms"
            );
        }
        byte[] bitmapBytes;
        try {
            bitmapBytes = Base64.getDecoder().decode(value);
        } catch (IllegalArgumentException e) {
            throw new IllegalArgumentException("[bitmap_terms] query expects a base64-encoded RoaringBitmap value", e);
        }
        RoaringBitmap bitmap = new RoaringBitmap();
        try {
            bitmap.deserialize(ByteBuffer.wrap(bitmapBytes).order(ByteOrder.LITTLE_ENDIAN));
            bitmap.validate();
        } catch (Exception e) {
            throw new IllegalArgumentException("[bitmap_terms] query value is not a valid serialized RoaringBitmap", e);
        }
        if (bitmap.isEmpty() == false && bitmap.last() < 0) {
            throw new IllegalArgumentException(
                "[bitmap_terms] query on [integer] field only supports non-negative values (0 to 2147483647)"
            );
        }
        if (numberFieldType.isIndexedWithTerms()) {
            return new IntBitmapIndexTermsQuery(fieldName, bitmap);
        }
        return new IntBitmapIndexBKDQuery(fieldName, bitmap);
    }

    @Override
    protected int doHashCode() {
        return Objects.hash(fieldName, value);
    }

    @Override
    protected boolean doEquals(BitmapTermsQueryBuilder other) {
        return Objects.equals(fieldName, other.fieldName) && Objects.equals(value, other.value);
    }

    @Override
    public TransportVersion getMinimalSupportedVersion() {
        return BITMAP_TERMS_QUERY_ADDED;
    }
}
