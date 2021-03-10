/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.index.mapper;

import org.apache.lucene.document.DoubleRange;
import org.apache.lucene.document.FloatRange;
import org.apache.lucene.document.InetAddressRange;
import org.apache.lucene.document.IntRange;
import org.apache.lucene.document.LongPoint;
import org.apache.lucene.document.LongRange;
import org.apache.lucene.document.SortedNumericDocValuesField;
import org.apache.lucene.queries.BinaryDocValuesRangeQuery;
import org.apache.lucene.search.IndexOrDocValuesQuery;
import org.apache.lucene.search.PointRangeQuery;
import org.apache.lucene.search.Query;
import org.elasticsearch.action.admin.indices.mapping.put.PutMappingRequest;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.compress.CompressedXContent;
import org.elasticsearch.common.network.InetAddresses;
import org.elasticsearch.common.time.DateMathParser;
import org.elasticsearch.index.query.SearchExecutionContext;
import org.elasticsearch.index.query.QueryStringQueryBuilder;
import org.elasticsearch.test.AbstractQueryTestCase;

import java.io.IOException;
import java.net.InetAddress;

import static org.hamcrest.Matchers.either;
import static org.hamcrest.core.IsInstanceOf.instanceOf;

public class RangeFieldQueryStringQueryBuilderTests extends AbstractQueryTestCase<QueryStringQueryBuilder> {

    private static final String INTEGER_RANGE_FIELD_NAME = "mapped_int_range";
    private static final String LONG_RANGE_FIELD_NAME = "mapped_long_range";
    private static final String FLOAT_RANGE_FIELD_NAME = "mapped_float_range";
    private static final String DOUBLE_RANGE_FIELD_NAME = "mapped_double_range";
    private static final String DATE_RANGE_FIELD_NAME = "mapped_date_range";
    private static final String IP_RANGE_FIELD_NAME = "mapped_ip_range";

    @Override
    protected void initializeAdditionalMappings(MapperService mapperService) throws IOException {
        mapperService.merge("_doc", new CompressedXContent(Strings.toString(PutMappingRequest.simpleMapping(
            INTEGER_RANGE_FIELD_NAME, "type=integer_range",
            LONG_RANGE_FIELD_NAME, "type=long_range",
            FLOAT_RANGE_FIELD_NAME, "type=float_range",
            DOUBLE_RANGE_FIELD_NAME, "type=double_range",
            DATE_RANGE_FIELD_NAME, "type=date_range",
            IP_RANGE_FIELD_NAME, "type=ip_range"
        ))), MapperService.MergeReason.MAPPING_UPDATE);

    }

    public void testIntegerRangeQuery() throws Exception {
        Query query = new QueryStringQueryBuilder(INTEGER_RANGE_FIELD_NAME + ":[-450 TO 45000]").toQuery(createSearchExecutionContext());
        Query range = IntRange.newIntersectsQuery(INTEGER_RANGE_FIELD_NAME, new int[]{-450}, new int[]{45000});
        Query dv = RangeType.INTEGER.dvRangeQuery(INTEGER_RANGE_FIELD_NAME,
            BinaryDocValuesRangeQuery.QueryType.INTERSECTS, -450, 45000, true, true);
        assertEquals(new IndexOrDocValuesQuery(range, dv), query);
    }

    public void testLongRangeQuery() throws Exception {
        Query query = new QueryStringQueryBuilder(LONG_RANGE_FIELD_NAME + ":[-450 TO 45000]").toQuery(createSearchExecutionContext());
        Query range = LongRange.newIntersectsQuery(LONG_RANGE_FIELD_NAME, new long[]{-450}, new long[]{45000});
        Query dv = RangeType.LONG.dvRangeQuery(LONG_RANGE_FIELD_NAME,
            BinaryDocValuesRangeQuery.QueryType.INTERSECTS, -450, 45000, true, true);
        assertEquals(new IndexOrDocValuesQuery(range, dv), query);
    }

    public void testFloatRangeQuery() throws Exception {
        Query query = new QueryStringQueryBuilder(FLOAT_RANGE_FIELD_NAME + ":[-450 TO 45000]").toQuery(createSearchExecutionContext());
        Query range = FloatRange.newIntersectsQuery(FLOAT_RANGE_FIELD_NAME, new float[]{-450}, new float[]{45000});
        Query dv = RangeType.FLOAT.dvRangeQuery(FLOAT_RANGE_FIELD_NAME,
            BinaryDocValuesRangeQuery.QueryType.INTERSECTS, -450.0f, 45000.0f, true, true);
        assertEquals(new IndexOrDocValuesQuery(range, dv), query);
    }

    public void testDoubleRangeQuery() throws Exception {
        Query query = new QueryStringQueryBuilder(DOUBLE_RANGE_FIELD_NAME + ":[-450 TO 45000]").toQuery(createSearchExecutionContext());
        Query range = DoubleRange.newIntersectsQuery(DOUBLE_RANGE_FIELD_NAME, new double[]{-450}, new double[]{45000});
        Query dv = RangeType.DOUBLE.dvRangeQuery(DOUBLE_RANGE_FIELD_NAME,
            BinaryDocValuesRangeQuery.QueryType.INTERSECTS, -450.0, 45000.0, true, true);
        assertEquals(new IndexOrDocValuesQuery(range, dv), query);
    }

    public void testDateRangeQuery() throws Exception {
        SearchExecutionContext context = createSearchExecutionContext();
        RangeFieldMapper.RangeFieldType type = (RangeFieldMapper.RangeFieldType) context.getFieldType(DATE_RANGE_FIELD_NAME);
        DateMathParser parser = type.dateMathParser;
        Query query = new QueryStringQueryBuilder(DATE_RANGE_FIELD_NAME + ":[2010-01-01 TO 2018-01-01]")
            .toQuery(createSearchExecutionContext());
        String lowerBoundExact = "2010-01-01T00:00:00.000";
        String upperBoundExact = "2018-01-01T23:59:59.999";
        Query range = LongRange.newIntersectsQuery(DATE_RANGE_FIELD_NAME,
            new long[]{ parser.parse(lowerBoundExact, () -> 0).toEpochMilli()},
            new long[]{ parser.parse(upperBoundExact, () -> 0).toEpochMilli()});
        Query dv = RangeType.DATE.dvRangeQuery(DATE_RANGE_FIELD_NAME,
            BinaryDocValuesRangeQuery.QueryType.INTERSECTS,
            parser.parse(lowerBoundExact, () -> 0).toEpochMilli(),
            parser.parse(upperBoundExact, () -> 0).toEpochMilli(), true, true);
        assertEquals(new IndexOrDocValuesQuery(range, dv), query);

        // also make sure the produced bounds are the same as on a regular `date` field
        DateFieldMapper.DateFieldType dateType = (DateFieldMapper.DateFieldType) context.getFieldType(DATE_FIELD_NAME);
        parser = dateType.dateMathParser;
        Query queryOnDateField = new QueryStringQueryBuilder(DATE_FIELD_NAME + ":[2010-01-01 TO 2018-01-01]")
            .toQuery(createSearchExecutionContext());
        Query controlQuery = LongPoint.newRangeQuery(DATE_FIELD_NAME,
                new long[]{ parser.parse(lowerBoundExact, () -> 0).toEpochMilli()},
                new long[]{ parser.parse(upperBoundExact, () -> 0).toEpochMilli()});

        Query controlDv = SortedNumericDocValuesField.newSlowRangeQuery(DATE_FIELD_NAME,
                parser.parse(lowerBoundExact, () -> 0).toEpochMilli(),
                parser.parse(upperBoundExact, () -> 0).toEpochMilli());
        assertEquals(new IndexOrDocValuesQuery(controlQuery, controlDv), queryOnDateField);
    }

    public void testIPRangeQuery() throws Exception {
        InetAddress lower = InetAddresses.forString("192.168.0.1");
        InetAddress upper = InetAddresses.forString("192.168.0.5");
        Query query = new QueryStringQueryBuilder(IP_RANGE_FIELD_NAME + ":[192.168.0.1 TO 192.168.0.5]")
            .toQuery(createSearchExecutionContext());
        Query range = InetAddressRange.newIntersectsQuery(IP_RANGE_FIELD_NAME, lower, upper);
        Query dv = RangeType.IP.dvRangeQuery(IP_RANGE_FIELD_NAME,
            BinaryDocValuesRangeQuery.QueryType.INTERSECTS,
            lower, upper, true, true);
        assertEquals(new IndexOrDocValuesQuery(range, dv), query);
    }

    @Override
    protected QueryStringQueryBuilder doCreateTestQueryBuilder() {
        return new QueryStringQueryBuilder(INTEGER_RANGE_FIELD_NAME + ":[-450 TO 450]");
    }

    @Override
    protected void doAssertLuceneQuery(QueryStringQueryBuilder queryBuilder, Query query, SearchExecutionContext context) {
        assertThat(query, either(instanceOf(PointRangeQuery.class)).or(instanceOf(IndexOrDocValuesQuery.class)));
    }
}
