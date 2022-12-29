/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.index.mapper;

import org.apache.lucene.document.Field;
import org.apache.lucene.document.StoredField;
import org.apache.lucene.index.IndexableField;
import org.apache.lucene.search.Query;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.common.geo.ShapeRelation;
import org.elasticsearch.common.time.DateFormatter;
import org.elasticsearch.common.time.DateMathParser;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.index.query.SearchExecutionContext;
import org.elasticsearch.xcontent.XContentParser;

import java.io.IOException;
import java.time.ZoneId;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.function.Function;

public interface RangeType {

    String getName();

    CoreRangeType.LengthType getLengthType();

    List<RangeFieldMapper.Range> decodeRanges(BytesRef bytes) throws IOException;

    Double doubleValue(Object endpointValue);

    BytesRef encodeRanges(Set<RangeFieldMapper.Range> ranges) throws IOException;

    Query rangeQuery(
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
    );

    Object minValue();

    Object maxValue();

    Object nextUp(Object value);

    Object nextDown(Object value);

    Query withinQuery(String field, Object from, Object to, boolean includeFrom, boolean includeTo);

    Query containsQuery(String field, Object from, Object to, boolean includeFrom, boolean includeTo);

    Query intersectsQuery(String field, Object from, Object to, boolean includeFrom, boolean includeTo);

    Object parseValue(RangeFieldMapper.RangeFieldType fieldType, XContentParser parser, boolean coerce, Function<Object, Object> rounding)
        throws IOException;

    Field getRangeField(String name, RangeFieldMapper.Range range);

    Object parseValue(Object value, boolean coerce, @Nullable DateMathParser dateMathParser);

    default List<IndexableField> createFields(
        DocumentParserContext context,
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

    default Object formatValue(Object value, DateFormatter formatter) {
        return value;
    }

    /**
     * most range types are numeric, overwrite if the implementing range type is not numeric, e.g. ip oder version_range do this
     */
    default boolean isNumeric() {
        return true;
    }

}
