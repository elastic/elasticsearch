/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.index.mapper;

import org.apache.lucene.document.Binary16RangeField;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.StoredField;
import org.apache.lucene.index.IndexableField;
import org.apache.lucene.queries.BinaryDocValuesRangeQuery;
import org.apache.lucene.search.IndexOrDocValuesQuery;
import org.apache.lucene.search.MatchNoDocsQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.store.ByteArrayDataOutput;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.geo.ShapeRelation;
import org.elasticsearch.common.time.DateMathParser;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.index.mapper.BasicRangeType.LengthType;
import org.elasticsearch.index.mapper.RangeFieldMapper.Range;
import org.elasticsearch.index.query.QueryShardContext;
import org.elasticsearch.xpack.versionfield.VersionEncoder;

import java.io.IOException;
import java.time.ZoneId;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Set;
import java.util.function.BiFunction;

/** Enum defining the type of range */
public class VersionRangeType implements RangeType {

    public static final String VERSION_RANGE_NAME = "version_range";

    public static final RangeType INSTANCE = new VersionRangeType();

    private BytesRef MIN_VALUE = new BytesRef();
    private BytesRef MAX_VALUE = new BytesRef(new byte[] { -1 });

    private VersionRangeType() {
        // singleton
    }

    public Field getRangeField(String name, RangeFieldMapper.Range r) {
        return new Binary16RangeField(name, (BytesRef) r.getFrom(), (BytesRef) r.getTo());
    }

    @Override
    public BytesRef parseFrom(RangeFieldMapper.RangeFieldType fieldType, XContentParser parser, boolean coerce, boolean included)
        throws IOException {
        BytesRef version = VersionEncoder.encodeVersion(parser.text());
        return included ? version : nextUp(version);
    }

    @Override
    public BytesRef parseTo(RangeFieldMapper.RangeFieldType fieldType, XContentParser parser, boolean coerce, boolean included)
        throws IOException {
        BytesRef version = VersionEncoder.encodeVersion(parser.text());
        return included ? version : nextDown(version);
    }

    @Override
    public BytesRef minValue() {
        return MIN_VALUE;
    }

    @Override
    public BytesRef maxValue() {
        return MAX_VALUE;
    }

    @Override
    public BytesRef encodeRanges(Set<Range> ranges) throws IOException {
        int length = 0;
        for (RangeFieldMapper.Range range : ranges) {
            length += ((BytesRef) range.getFrom()).length;
            length += ((BytesRef) range.getTo()).length;
        }
        final byte[] encoded = new byte[15 + length];
        ByteArrayDataOutput out = new ByteArrayDataOutput(encoded);
        out.writeVInt(ranges.size());
        for (RangeFieldMapper.Range range : ranges) {
            BytesRef fromValue = (BytesRef) range.getFrom();
            byte[] encodedFromValue = fromValue.bytes;
            int fromLength = ((BytesRef) range.getFrom()).length;
            out.writeByte((byte) fromLength);
            out.writeBytes(encodedFromValue, 0, fromLength);

            BytesRef toValue = (BytesRef) range.getTo();
            byte[] encodedToValue = toValue.bytes;
            int toLength = ((BytesRef) range.getTo()).length;
            out.writeByte((byte) toLength);
            out.writeBytes(encodedToValue, 0, toLength);
        }
        return new BytesRef(encoded, 0, out.getPosition());
    }

    @Override
    public List<RangeFieldMapper.Range> decodeRanges(BytesRef bytes) {
        throw new UnsupportedOperationException();
    }

    @Override
    public Double doubleValue(Object endpointValue) {
        throw new UnsupportedOperationException("Version ranges cannot be safely converted to doubles");
    }

    @Override
    public List<IndexableField> createFields(
        ParseContext context,
        String name,
        RangeFieldMapper.Range range,
        boolean indexed,
        boolean docValued,
        boolean stored
    ) {
        assert range != null : "range cannot be null when creating fields";
        List<IndexableField> fields = new ArrayList<>();
        if (indexed) {
            fields.add(getRangeField(name, range));
        }
        if (docValued) {
            RangeFieldMapper.BinaryRangesDocValuesField field = (RangeFieldMapper.BinaryRangesDocValuesField) context.doc().getByKey(name);
            if (field == null) {
                field = new RangeFieldMapper.BinaryRangesDocValuesField(name, range, this);
                context.doc().addWithKey(name, field);
            } else {
                field.add(range);
            }
        }
        if (stored) {
            fields.add(new StoredField(name, range.toString()));
        }
        return fields;
    }

    @Override
    public Query rangeQuery(
        String field,
        boolean hasDocValues,
        Object from,
        Object to,
        boolean includeFrom,
        boolean includeTo,
        ShapeRelation relation,
        @Nullable ZoneId timeZone,
        @Nullable DateMathParser dateMathParser,
        QueryShardContext context
    ) {
        Object lower = from == null ? minValue() : parse(from, false);
        Object upper = to == null ? maxValue() : parse(to, false);
        return createRangeQuery(field, hasDocValues, lower, upper, includeFrom, includeTo, relation);
    }

    private BytesRef parse(Object value, boolean coerce) {
        if (value instanceof BytesRef) {
            value = ((BytesRef) value).utf8ToString();
        }
        return VersionEncoder.encodeVersion(value.toString());
    }

    protected final Query createRangeQuery(
        String field,
        boolean hasDocValues,
        Object lower,
        Object upper,
        boolean includeFrom,
        boolean includeTo,
        ShapeRelation relation
    ) {
        Query indexQuery;
        if (relation == ShapeRelation.WITHIN) {
            indexQuery = withinQuery(field, lower, upper, includeFrom, includeTo);
        } else if (relation == ShapeRelation.CONTAINS) {
            indexQuery = containsQuery(field, lower, upper, includeFrom, includeTo);
        } else {
            indexQuery = intersectsQuery(field, lower, upper, includeFrom, includeTo);
        }
        if (hasDocValues) {
            final BinaryDocValuesRangeQuery.QueryType queryType;
            if (relation == ShapeRelation.WITHIN) {
                queryType = BinaryDocValuesRangeQuery.QueryType.WITHIN;
            } else if (relation == ShapeRelation.CONTAINS) {
                queryType = BinaryDocValuesRangeQuery.QueryType.CONTAINS;
            } else {
                queryType = BinaryDocValuesRangeQuery.QueryType.INTERSECTS;
            }
            Query dvQuery = dvRangeQuery(field, queryType, lower, upper, includeFrom, includeTo);
            return new IndexOrDocValuesQuery(indexQuery, dvQuery);
        } else {
            return indexQuery;
        }
    }

    @Override
    public Query withinQuery(String field, Object from, Object to, boolean includeFrom, boolean includeTo) {
        return createQuery(
            field,
            (BytesRef) from,
            (BytesRef) to,
            includeFrom,
            includeTo,
            (f, t) -> Binary16RangeField.newWithinQuery(
                field,
                f,
                t,
                dvRangeQuery(field, BinaryDocValuesRangeQuery.QueryType.WITHIN, f, t, includeFrom, includeTo)
            )
        );
    }

    @Override
    public Query containsQuery(String field, Object from, Object to, boolean includeFrom, boolean includeTo) {
        return createQuery(
            field,
            (BytesRef) from,
            (BytesRef) to,
            includeFrom,
            includeTo,
            (f, t) -> Binary16RangeField.newContainsQuery(
                field,
                f,
                t,
                dvRangeQuery(field, BinaryDocValuesRangeQuery.QueryType.CONTAINS, f, t, includeFrom, includeTo)
            )
        );
    }

    @Override
    public Query intersectsQuery(String field, Object from, Object to, boolean includeFrom, boolean includeTo) {
        return createQuery(
            field,
            (BytesRef) from,
            (BytesRef) to,
            includeFrom,
            includeTo,
            (f, t) -> Binary16RangeField.newIntersectsQuery(
                field,
                f,
                t,
                dvRangeQuery(field, BinaryDocValuesRangeQuery.QueryType.INTERSECTS, f, t, includeFrom, includeTo)
            )
        );
    }

    private Query createQuery(
        String field,
        BytesRef lower,
        BytesRef upper,
        boolean includeFrom,
        boolean includeTo,
        BiFunction<BytesRef, BytesRef, Query> querySupplier
    ) {
        if (lower.compareTo(upper) > 0) {
            throw new IllegalArgumentException(
                "Version range query `from` value (" + lower + ") is greater than `to` value (" + upper + ")"
            );
        }
        BytesRef correctedFrom = includeFrom ? lower : nextUp(lower);
        BytesRef correctedTo = includeTo ? upper : nextDown(upper);
        ;
        byte[] lowerBytes = correctedFrom.bytes;
        byte[] upperBytes = correctedTo.bytes;
        if (Arrays.compareUnsigned(lowerBytes, 0, lowerBytes.length, upperBytes, 0, upperBytes.length) > 0) {
            return new MatchNoDocsQuery("version range didn't intersect anything");
        } else {
            return querySupplier.apply(lower, upper);
        }
    }

    @Override
    public BytesRef nextUp(Object value) {
        BytesRef nextUp = BytesRef.deepCopyOf(((BytesRef) value));
        // adding 1 to last byte should not overflow since values 128-255 are reserved for lengths and cannot come last
        nextUp.bytes[nextUp.offset + nextUp.length - 1] = (byte) (nextUp.bytes[nextUp.offset + nextUp.length - 1] + 1);
        return nextUp;
    }

    @Override
    public BytesRef nextDown(Object value) {
        BytesRef nextDown = BytesRef.deepCopyOf(((BytesRef) value));
        // subtracting 1 from last byte should not underflow since 0 shouldn't be last byte in encoded version
        nextDown.bytes[nextDown.offset + nextDown.length - 1] = (byte) (nextDown.bytes[nextDown.offset + nextDown.length - 1] - 1);
        return nextDown;
    }

    private Query dvRangeQuery(
        String field,
        BinaryDocValuesRangeQuery.QueryType queryType,
        Object from,
        Object to,
        boolean includeFrom,
        boolean includeTo
    ) {
        BytesRef encodedFrom = (BytesRef) from;
        BytesRef encodedTo = (BytesRef) to;
        if (includeFrom == false) {
            encodedFrom = nextUp(encodedFrom);
        }

        if (includeTo == false) {
            encodedTo = nextDown(encodedTo);
        }
        return new BinaryDocValuesRangeQuery(field, queryType, LengthType.FULL_BYTE, encodedFrom, encodedTo, from, to);
    }

    @Override
    public boolean isNumeric() {
        return false;
    }

    @Override
    public String getName() {
        return VERSION_RANGE_NAME;
    };

    @Override
    public BasicRangeType.LengthType getLengthType() {
        return BasicRangeType.LengthType.VARIABLE;
    };

}
