/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.expression.function.scalar.convert;

import com.carrotsearch.randomizedtesting.annotations.Name;
import com.carrotsearch.randomizedtesting.annotations.ParametersFactory;

import org.apache.lucene.util.BytesRef;
import org.elasticsearch.common.time.DateUtils;
import org.elasticsearch.index.mapper.DateFieldMapper;
import org.elasticsearch.search.DocValueFormat;
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.expression.function.TestCaseSupplier;
import org.elasticsearch.xpack.esql.expression.function.scalar.AbstractConfigurationFunctionTestCase;
import org.elasticsearch.xpack.esql.session.Configuration;
import org.elasticsearch.xpack.esql.type.EsqlDataTypeConverter;

import java.math.BigInteger;
import java.time.Instant;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.List;
import java.util.function.Supplier;

import static org.elasticsearch.test.ReadableMatchers.matchesBytesRef;
import static org.elasticsearch.xpack.esql.core.util.SpatialCoordinateTypes.CARTESIAN;
import static org.elasticsearch.xpack.esql.core.util.SpatialCoordinateTypes.GEO;
import static org.elasticsearch.xpack.esql.expression.function.TestCaseSupplier.TEST_SOURCE;

public class ToStringTests extends AbstractConfigurationFunctionTestCase {
    public ToStringTests(@Name("TestCase") Supplier<TestCaseSupplier.TestCase> testCaseSupplier) {
        this.testCase = testCaseSupplier.get();
    }

    @ParametersFactory
    public static Iterable<Object[]> parameters() {
        // TODO multivalue fields
        String read = "Attribute[channel=0]";
        List<TestCaseSupplier> suppliers = new ArrayList<>();
        TestCaseSupplier.unary(
            suppliers,
            "ToStringFromDatetimeEvaluator[datetime=" + read + ", formatter=format[strict_date_optional_time] locale[]]",
            TestCaseSupplier.dateCases(),
            DataType.KEYWORD,
            i -> matchesBytesRef(DateFieldMapper.DEFAULT_DATE_TIME_FORMATTER.formatMillis(DateUtils.toLongMillis((Instant) i))),
            List.of()
        );
        TestCaseSupplier.unary(
            suppliers,
            "ToStringFromDateNanosEvaluator[datetime=" + read + ", formatter=format[strict_date_optional_time_nanos] locale[]]",
            TestCaseSupplier.dateNanosCases(),
            DataType.KEYWORD,
            i -> matchesBytesRef(DateFieldMapper.DEFAULT_DATE_TIME_NANOS_FORMATTER.formatNanos(DateUtils.toLong((Instant) i))),
            List.of()
        );
        // Set the config to UTC for date cases
        suppliers = TestCaseSupplier.mapTestCases(
            suppliers,
            tc -> tc.withConfiguration(TEST_SOURCE, configurationForTimezone(ZoneOffset.UTC))
        );

        TestCaseSupplier.forUnaryInt(
            suppliers,
            "ToStringFromIntEvaluator[integer=" + read + "]",
            DataType.KEYWORD,
            i -> matchesBytesRef(Integer.toString(i)),
            Integer.MIN_VALUE,
            Integer.MAX_VALUE,
            List.of()
        );
        TestCaseSupplier.forUnaryLong(
            suppliers,
            "ToStringFromLongEvaluator[lng=" + read + "]",
            DataType.KEYWORD,
            l -> matchesBytesRef(Long.toString(l)),
            Long.MIN_VALUE,
            Long.MAX_VALUE,
            List.of()
        );
        TestCaseSupplier.forUnaryUnsignedLong(
            suppliers,
            "ToStringFromUnsignedLongEvaluator[lng=" + read + "]",
            DataType.KEYWORD,
            ul -> matchesBytesRef(ul.toString()),
            BigInteger.ZERO,
            UNSIGNED_LONG_MAX,
            List.of()
        );
        TestCaseSupplier.forUnaryDouble(
            suppliers,
            "ToStringFromDoubleEvaluator[dbl=" + read + "]",
            DataType.KEYWORD,
            d -> matchesBytesRef(Double.toString(d)),
            Double.NEGATIVE_INFINITY,
            Double.POSITIVE_INFINITY,
            List.of()
        );
        TestCaseSupplier.forUnaryDenseVector(
            suppliers,
            "ToStringFromFloatEvaluator[flt=" + read + "]",
            DataType.KEYWORD,
            d -> d.stream().map(f -> new BytesRef(f.toString())).toList(),
            -1.0f,
            1.0f
        );
        TestCaseSupplier.forUnaryBoolean(
            suppliers,
            "ToStringFromBooleanEvaluator[bool=" + read + "]",
            DataType.KEYWORD,
            b -> matchesBytesRef(b.toString()),
            List.of()
        );
        TestCaseSupplier.forUnaryGeoPoint(
            suppliers,
            "ToStringFromGeoPointEvaluator[wkb=" + read + "]",
            DataType.KEYWORD,
            wkb -> matchesBytesRef(GEO.wkbToWkt(wkb)),
            List.of()
        );
        TestCaseSupplier.forUnaryCartesianPoint(
            suppliers,
            "ToStringFromCartesianPointEvaluator[wkb=" + read + "]",
            DataType.KEYWORD,
            wkb -> matchesBytesRef(CARTESIAN.wkbToWkt(wkb)),
            List.of()
        );
        TestCaseSupplier.forUnaryGeoShape(
            suppliers,
            "ToStringFromGeoShapeEvaluator[wkb=" + read + "]",
            DataType.KEYWORD,
            wkb -> matchesBytesRef(GEO.wkbToWkt(wkb)),
            List.of()
        );
        TestCaseSupplier.forUnaryCartesianShape(
            suppliers,
            "ToStringFromCartesianShapeEvaluator[wkb=" + read + "]",
            DataType.KEYWORD,
            wkb -> matchesBytesRef(CARTESIAN.wkbToWkt(wkb)),
            List.of()
        );
        TestCaseSupplier.forUnaryIp(
            suppliers,
            "ToStringFromIPEvaluator[ip=" + read + "]",
            DataType.KEYWORD,
            ip -> matchesBytesRef(DocValueFormat.IP.format(ip)),
            List.of()
        );
        TestCaseSupplier.forUnaryStrings(suppliers, read, DataType.KEYWORD, bytesRef -> bytesRef, List.of());
        TestCaseSupplier.forUnaryVersion(
            suppliers,
            "ToStringFromVersionEvaluator[version=" + read + "]",
            DataType.KEYWORD,
            v -> matchesBytesRef(v.toString()),
            List.of()
        );
        // Geo-Grid types
        for (DataType gridType : new DataType[] { DataType.GEOHASH, DataType.GEOTILE, DataType.GEOHEX }) {
            TestCaseSupplier.forUnaryGeoGrid(
                suppliers,
                "ToStringFromGeoGridEvaluator[gridId=Attribute[channel=0], dataType=" + gridType + "]",
                gridType,
                DataType.KEYWORD,
                v -> matchesBytesRef(EsqlDataTypeConverter.geoGridToString((long) v, gridType)),
                List.of()
            );
        }
        TestCaseSupplier.forUnaryAggregateMetricDouble(
            suppliers,
            "ToStringFromAggregateMetricDoubleEvaluator[field=" + read + "]",
            DataType.KEYWORD,
            agg -> matchesBytesRef(EsqlDataTypeConverter.aggregateMetricDoubleLiteralToString(agg)),
            List.of()
        );
        TestCaseSupplier.forUnaryExponentialHistogram(
            suppliers,
            "ToStringFromExponentialHistogramEvaluator[histogram=" + read + "]",
            DataType.KEYWORD,
            eh -> new BytesRef(EsqlDataTypeConverter.exponentialHistogramToString(eh)),
            List.of()
        );

        suppliers.addAll(casesForDate("2020-02-03T10:12:14Z", "Z", "2020-02-03T10:12:14.000Z"));
        suppliers.addAll(casesForDate("2020-02-03T10:12:14+01:00", "Europe/Madrid", "2020-02-03T10:12:14.000+01:00"));
        suppliers.addAll(casesForDate("2020-06-30T10:12:14+02:00", "Europe/Madrid", "2020-06-30T10:12:14.000+02:00"));

        return parameterSuppliersFromTypedDataWithDefaultChecks(true, suppliers);
    }

    private static List<TestCaseSupplier> casesForDate(String date, String zoneIdString, String expectedString) {
        ZoneId zoneId = ZoneId.of(zoneIdString);
        long dateAsLong = DateFieldMapper.DEFAULT_DATE_TIME_FORMATTER.parseMillis(date);
        long dateAsNanos = DateUtils.toNanoSeconds(dateAsLong);

        return List.of(
            new TestCaseSupplier(
                "millis: " + date + ", " + zoneIdString + ", " + expectedString,
                List.of(DataType.DATETIME),
                () -> new TestCaseSupplier.TestCase(
                    List.of(new TestCaseSupplier.TypedData(dateAsLong, DataType.DATETIME, "date")),
                    "ToStringFromDatetimeEvaluator[datetime=Attribute[channel=0], "
                        + "formatter=format[strict_date_optional_time] locale[]]",
                    DataType.KEYWORD,
                    matchesBytesRef(expectedString)
                ).withConfiguration(TEST_SOURCE, configurationForTimezone(zoneId))
            ),

            new TestCaseSupplier(
                "nanos: " + date + ", " + zoneIdString + ", " + expectedString,
                List.of(DataType.DATE_NANOS),
                () -> new TestCaseSupplier.TestCase(
                    List.of(new TestCaseSupplier.TypedData(dateAsNanos, DataType.DATE_NANOS, "date")),
                    "ToStringFromDateNanosEvaluator[datetime=Attribute[channel=0], "
                        + "formatter=format[strict_date_optional_time_nanos] locale[]]",
                    DataType.KEYWORD,
                    matchesBytesRef(expectedString)
                ).withConfiguration(TEST_SOURCE, configurationForTimezone(zoneId))
            )
        );
    }

    @Override
    protected Expression buildWithConfiguration(Source source, List<Expression> args, Configuration configuration) {
        return new ToString(source, args.get(0), configuration);
    }
}
