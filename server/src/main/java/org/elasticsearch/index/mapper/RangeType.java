package org.elasticsearch.index.mapper;

import org.apache.lucene.index.IndexableField;
import org.apache.lucene.search.Query;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.geo.ShapeRelation;
import org.elasticsearch.common.time.DateMathParser;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.index.query.QueryShardContext;

import java.io.IOException;
import java.time.ZoneId;
import java.util.List;
import java.util.Set;

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
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANYDa
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

public interface RangeType {

    String getName();

    BasicRangeType.LengthType getLengthType();

    boolean isNumeric();

    List<RangeFieldMapper.Range> decodeRanges(BytesRef bytes);

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
        QueryShardContext context
    );

    Object minValue();

    Object maxValue();

    Object nextUp(Object value);

    Object nextDown(Object value);

    Query withinQuery(String field, Object from, Object to, boolean includeFrom, boolean includeTo);

    Query containsQuery(String field, Object from, Object to, boolean includeFrom, boolean includeTo);

    Query intersectsQuery(String field, Object from, Object to, boolean includeFrom, boolean includeTo);

    Object parseFrom(RangeFieldMapper.RangeFieldType fieldType, XContentParser parser, boolean coerce, boolean included) throws IOException;

    Object parseTo(RangeFieldMapper.RangeFieldType fieldType, XContentParser parser, boolean coerce, boolean included) throws IOException;

    List<IndexableField> createFields(
        ParseContext context,
        String name,
        RangeFieldMapper.Range range,
        boolean indexed,
        boolean docValued,
        boolean stored
    );
}
