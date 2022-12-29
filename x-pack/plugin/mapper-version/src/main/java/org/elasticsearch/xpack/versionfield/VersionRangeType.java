/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.versionfield;

import org.apache.lucene.document.Field;
import org.apache.lucene.search.BooleanClause;
import org.apache.lucene.search.BooleanClause.Occur;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.MatchNoDocsQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.store.ByteArrayDataOutput;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.common.geo.ShapeRelation;
import org.elasticsearch.common.time.DateMathParser;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.index.mapper.CoreRangeType;
import org.elasticsearch.index.mapper.CoreRangeType.LengthType;
import org.elasticsearch.index.mapper.FieldMapper;
import org.elasticsearch.index.mapper.FieldMapper.TypeParser;
import org.elasticsearch.index.mapper.RangeFieldMapper;
import org.elasticsearch.index.mapper.RangeFieldMapper.RangeFieldType;
import org.elasticsearch.index.mapper.RangeType;
import org.elasticsearch.index.query.SearchExecutionContext;
import org.elasticsearch.lucene.queries.BinaryDocValuesRangeQuery;
import org.elasticsearch.percolator.BinaryRange;

import java.io.IOException;
import java.time.ZoneId;
import java.util.Arrays;
import java.util.List;
import java.util.Set;
import java.util.function.BiFunction;
import java.util.function.Function;

import static org.elasticsearch.percolator.BinaryRange.BYTES;

public class VersionRangeType implements RangeType {

    public static final String VERSION_RANGE_NAME = "version_range";
    private static final RangeType INSTANCE = new VersionRangeType();
    static final TypeParser PARSER = new FieldMapper.TypeParser(
        (n, c) -> new RangeFieldMapper.Builder(n, INSTANCE, c.getSettings())
    );

    private BytesRef MIN_VALUE = new BytesRef();
    private BytesRef MAX_VALUE = new BytesRef(new byte[] { -1 });

    private VersionRangeType() {
        // singleton
    }

    @Override
    public Field getRangeField(String name, RangeFieldMapper.Range r) {
        return new BinaryRange(name, encode((BytesRef) r.getFrom(), (BytesRef) r.getTo()));
    }

    /** encode the min/max range and return the byte array */
    private static byte[] encode(BytesRef min, BytesRef max) {
        byte[] bytes = new byte[BYTES * 2];
        int minBytesLength = Math.min(min.length, BYTES);
        int maxBytesLength = Math.min(max.length, BYTES);
        if (Arrays.compareUnsigned(
            min.bytes,
            min.offset,
            min.offset + minBytesLength,
            max.bytes,
            max.offset,
            max.offset + maxBytesLength
        ) > 0) {
            throw new IllegalArgumentException("min value cannot be greater than max value for version field");
        }
        System.arraycopy(min.bytes, 0 + min.offset, bytes, 0, minBytesLength);
        System.arraycopy(max.bytes, 0 + max.offset, bytes, BYTES, maxBytesLength);
        return bytes;
    }

    @Override
    public BytesRef parseValue(Object value, boolean coerce, @Nullable DateMathParser dateMathParser) {
        return VersionEncoder.encodeVersion((String) value).bytesRef;
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
    public BytesRef encodeRanges(Set<RangeFieldMapper.Range> ranges) throws IOException {
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
        SearchExecutionContext context
    ) {
        Object lower = from == null ? minValue() : parse(from, false);
        Object upper = to == null ? maxValue() : parse(to, false);
        return createRangeQuery(field, hasDocValues, lower, upper, includeFrom, includeTo, relation);
    }

    private BytesRef parse(Object value, boolean coerce) {
        if (value instanceof BytesRef) {
            value = ((BytesRef) value).utf8ToString();
        }
        return VersionEncoder.encodeVersion(value.toString()).bytesRef;
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
        // if (hasDocValues == false) {
        //     throw new IllegalArgumentException("range queries on `version_range` fields require doc_values.");
        // }
        Query rangeQuery;
        if (relation == ShapeRelation.WITHIN) {
            rangeQuery = withinQuery(field, lower, upper, includeFrom, includeTo);
        } else if (relation == ShapeRelation.CONTAINS) {
            rangeQuery = containsQuery(field, lower, upper, includeFrom, includeTo);
        } else {
            rangeQuery = intersectsQuery(field, lower, upper, includeFrom, includeTo);
        }
        return rangeQuery;
    }

    @Override
    public Query withinQuery(String field, Object from, Object to, boolean includeFrom, boolean includeTo) {
        return createQuery(
            field,
            (BytesRef) from,
            (BytesRef) to,
            includeFrom,
            includeTo,
            (f, t) -> BinaryRange.newWithinQuery(field, encode(f, t)),
            dvRangeQuery(field, BinaryDocValuesRangeQuery.QueryType.WITHIN, from, to, includeFrom, includeTo)
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
            (f, t) -> BinaryRange.newContainsQuery(field, encode(f, t)),
            dvRangeQuery(field, BinaryDocValuesRangeQuery.QueryType.CONTAINS, from, to, includeFrom, includeTo)
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
            (f, t) -> BinaryRange.newIntersectsQuery(field, encode(f, t)),
            dvRangeQuery(field, BinaryDocValuesRangeQuery.QueryType.INTERSECTS, from, to, includeFrom, includeTo)
        );
    }

    private Query createQuery(
        String field,
        BytesRef lower,
        BytesRef upper,
        boolean includeFrom,
        boolean includeTo,
        BiFunction<BytesRef, BytesRef, Query> querySupplier,
        Query dvRangeQuery
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
            Query rangeFieldQuery = querySupplier.apply(lower, upper);
            BooleanQuery conjunctionQuery = new BooleanQuery.Builder().add(new BooleanClause(rangeFieldQuery, Occur.MUST))
                .add(new BooleanClause(dvRangeQuery, Occur.MUST))
                .build();
            return conjunctionQuery;
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
    public CoreRangeType.LengthType getLengthType() {
        return CoreRangeType.LengthType.VARIABLE;
    }

    @Override
    public Object parseValue(RangeFieldType fieldType, XContentParser parser, boolean coerce, Function<Object, Object> rounding)
        throws IOException {
        BytesRef version = parseValue(parser.text(), coerce, null);
        return rounding.apply(version);
    };

}
