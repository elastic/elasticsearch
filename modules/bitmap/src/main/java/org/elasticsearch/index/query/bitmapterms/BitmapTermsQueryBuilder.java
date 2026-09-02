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
import org.elasticsearch.index.query.LeafQueryBuilder;
import org.elasticsearch.index.query.SearchExecutionContext;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentParser;

import java.io.IOException;
import java.util.Base64;
import java.util.Objects;

/**
 * A query that matches documents whose numeric field value is present in a RoaringBitmap,
 * supplied as a base64-encoded serialized bitmap in little-endian byte order.
 * <p>
 * Only supported on indexed {@code integer} and {@code long} fields ({@code index: true}), and only
 * for non-negative values &mdash; 0 to 2,147,483,647 for {@code integer}, 0 to
 * 9,223,372,036,854,775,807 for {@code long}. The field's type selects the bitmap format:
 * <ul>
 *   <li>{@code integer} expects a 32-bit RoaringBitmap, as produced by
 *       {@code RoaringBitmap#serialize} or pyroaring's {@code BitMap#serialize}.</li>
 *   <li>{@code long} expects a 64-bit bitmap in the portable format, as produced by
 *       {@code Roaring64NavigableMap#serializePortable} or pyroaring's {@code BitMap64#serialize}
 *       (see {@link LongBitmap}). Java clients must call {@code serializePortable} rather than
 *       {@code serialize}, which defaults to a different layout.</li>
 * </ul>
 * Example:
 * <pre>{@code
 * {
 *   "bitmap_terms": {
 *     "field": "my_long_field",
 *     "value": "<base64-encoded bitmap>"
 *   }
 * }
 * }</pre>
 */
public class BitmapTermsQueryBuilder extends LeafQueryBuilder<BitmapTermsQueryBuilder> {

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
    protected Query doToQuery(SearchExecutionContext context) throws IOException {
        MappedFieldType fieldType = context.getFieldType(fieldName);
        if (!(fieldType instanceof NumberFieldMapper.NumberFieldType numberFieldType)
            || (numberFieldType.numberType() != NumberFieldMapper.NumberType.INTEGER
                && numberFieldType.numberType() != NumberFieldMapper.NumberType.LONG)
            || (numberFieldType.isIndexedWithPoints() == false && numberFieldType.isIndexedWithTerms() == false)) {
            throw new IllegalArgumentException(
                "[bitmap_terms] query is not supported on field ["
                    + fieldName
                    + "]: only supported on [integer] and [long] fields indexed with points or terms"
            );
        }
        byte[] bitmapBytes;
        try {
            bitmapBytes = Base64.getDecoder().decode(value);
        } catch (IllegalArgumentException e) {
            throw new IllegalArgumentException("[bitmap_terms] query expects a base64-encoded RoaringBitmap value", e);
        }
        BitmapValues values = switch (numberFieldType.numberType()) {
            case INTEGER -> integerValues(bitmapBytes);
            case LONG -> longValues(bitmapBytes);
            default -> throw new AssertionError("unexpected number type [" + numberFieldType.numberType() + "]");
        };
        // The two queries differ only in which index structure they merge against; the field's width
        // is carried by the BitmapValues.
        if (numberFieldType.isIndexedWithTerms()) {
            return new BitmapTermsQuery(fieldName, values);
        }
        return new BitmapBKDQuery(fieldName, values);
    }

    private static IntBitmap integerValues(byte[] bitmapBytes) {
        IntBitmap bitmap;
        try {
            bitmap = IntBitmap.deserialize(bitmapBytes);
        } catch (Exception e) {
            throw new IllegalArgumentException("[bitmap_terms] query value is not a valid serialized RoaringBitmap", e);
        }
        if (bitmap.hasNegativeValues()) {
            throw new IllegalArgumentException(
                "[bitmap_terms] query on [integer] field only supports non-negative values (0 to 2147483647)"
            );
        }
        return bitmap;
    }

    private static LongBitmap longValues(byte[] bitmapBytes) {
        LongBitmap bitmap;
        try {
            bitmap = LongBitmap.deserializePortable(bitmapBytes);
        } catch (Exception e) {
            throw new IllegalArgumentException(
                "[bitmap_terms] query value is not a valid serialized 64-bit RoaringBitmap in the portable format",
                e
            );
        }
        if (bitmap.hasNegativeValues()) {
            throw new IllegalArgumentException(
                "[bitmap_terms] query on [long] field only supports non-negative values (0 to 9223372036854775807)"
            );
        }
        return bitmap;
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
