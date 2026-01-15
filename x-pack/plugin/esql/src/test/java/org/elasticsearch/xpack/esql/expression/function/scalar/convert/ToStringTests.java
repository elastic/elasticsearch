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
import org.elasticsearch.exponentialhistogram.ExponentialHistogram;
import org.elasticsearch.exponentialhistogram.ExponentialHistogramBuilder;
import org.elasticsearch.exponentialhistogram.ExponentialHistogramCircuitBreaker;
import org.elasticsearch.index.mapper.DateFieldMapper;
import org.elasticsearch.search.DocValueFormat;
import org.elasticsearch.xpack.esql.WriteableExponentialHistogram;
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.expression.function.AbstractScalarFunctionTestCase;
import org.elasticsearch.xpack.esql.expression.function.TestCaseSupplier;
import org.elasticsearch.xpack.esql.type.EsqlDataTypeConverter;

import java.math.BigInteger;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.function.Supplier;

import static org.elasticsearch.xpack.esql.core.util.SpatialCoordinateTypes.CARTESIAN;
import static org.elasticsearch.xpack.esql.core.util.SpatialCoordinateTypes.GEO;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.nullValue;

public class ToStringTests extends AbstractScalarFunctionTestCase {
    public ToStringTests(@Name("TestCase") Supplier<TestCaseSupplier.TestCase> testCaseSupplier) {
        this.testCase = testCaseSupplier.get();
    }

    @ParametersFactory
    public static Iterable<Object[]> parameters() {
        // TODO multivalue fields
        String read = "Attribute[channel=0]";
        List<TestCaseSupplier> suppliers = new ArrayList<>();
        TestCaseSupplier.forUnaryInt(
            suppliers,
            "ToStringFromIntEvaluator[integer=" + read + "]",
            DataType.KEYWORD,
            i -> new BytesRef(Integer.toString(i)),
            Integer.MIN_VALUE,
            Integer.MAX_VALUE,
            List.of()
        );
        TestCaseSupplier.forUnaryLong(
            suppliers,
            "ToStringFromLongEvaluator[lng=" + read + "]",
            DataType.KEYWORD,
            l -> new BytesRef(Long.toString(l)),
            Long.MIN_VALUE,
            Long.MAX_VALUE,
            List.of()
        );
        TestCaseSupplier.forUnaryUnsignedLong(
            suppliers,
            "ToStringFromUnsignedLongEvaluator[lng=" + read + "]",
            DataType.KEYWORD,
            ul -> new BytesRef(ul.toString()),
            BigInteger.ZERO,
            UNSIGNED_LONG_MAX,
            List.of()
        );
        TestCaseSupplier.forUnaryDouble(
            suppliers,
            "ToStringFromDoubleEvaluator[dbl=" + read + "]",
            DataType.KEYWORD,
            d -> new BytesRef(Double.toString(d)),
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
            b -> new BytesRef(b.toString()),
            List.of()
        );
        TestCaseSupplier.unary(
            suppliers,
            "ToStringFromDatetimeEvaluator[datetime=" + read + "]",
            TestCaseSupplier.dateCases(),
            DataType.KEYWORD,
            i -> new BytesRef(DateFieldMapper.DEFAULT_DATE_TIME_FORMATTER.formatMillis(DateUtils.toLongMillis((Instant) i))),
            List.of()
        );
        TestCaseSupplier.unary(
            suppliers,
            "ToStringFromDateNanosEvaluator[datetime=" + read + "]",
            TestCaseSupplier.dateNanosCases(),
            DataType.KEYWORD,
            i -> new BytesRef(DateFieldMapper.DEFAULT_DATE_TIME_NANOS_FORMATTER.formatNanos(DateUtils.toLong((Instant) i))),
            List.of()
        );
        TestCaseSupplier.forUnaryGeoPoint(
            suppliers,
            "ToStringFromGeoPointEvaluator[wkb=" + read + "]",
            DataType.KEYWORD,
            wkb -> new BytesRef(GEO.wkbToWkt(wkb)),
            List.of()
        );
        TestCaseSupplier.forUnaryCartesianPoint(
            suppliers,
            "ToStringFromCartesianPointEvaluator[wkb=" + read + "]",
            DataType.KEYWORD,
            wkb -> new BytesRef(CARTESIAN.wkbToWkt(wkb)),
            List.of()
        );
        TestCaseSupplier.forUnaryGeoShape(
            suppliers,
            "ToStringFromGeoShapeEvaluator[wkb=" + read + "]",
            DataType.KEYWORD,
            wkb -> new BytesRef(GEO.wkbToWkt(wkb)),
            List.of()
        );
        TestCaseSupplier.forUnaryCartesianShape(
            suppliers,
            "ToStringFromCartesianShapeEvaluator[wkb=" + read + "]",
            DataType.KEYWORD,
            wkb -> new BytesRef(CARTESIAN.wkbToWkt(wkb)),
            List.of()
        );
        TestCaseSupplier.forUnaryIp(
            suppliers,
            "ToStringFromIPEvaluator[ip=" + read + "]",
            DataType.KEYWORD,
            ip -> new BytesRef(DocValueFormat.IP.format(ip)),
            List.of()
        );
        TestCaseSupplier.forUnaryStrings(suppliers, read, DataType.KEYWORD, bytesRef -> bytesRef, List.of());
        TestCaseSupplier.forUnaryVersion(
            suppliers,
            "ToStringFromVersionEvaluator[version=" + read + "]",
            DataType.KEYWORD,
            v -> new BytesRef(v.toString()),
            List.of()
        );
        // Geo-Grid types
        for (DataType gridType : new DataType[] { DataType.GEOHASH, DataType.GEOTILE, DataType.GEOHEX }) {
            TestCaseSupplier.forUnaryGeoGrid(
                suppliers,
                "ToStringFromGeoGridEvaluator[gridId=Attribute[channel=0], dataType=" + gridType + "]",
                gridType,
                DataType.KEYWORD,
                v -> new BytesRef(EsqlDataTypeConverter.geoGridToString((long) v, gridType)),
                List.of()
            );
        }
        TestCaseSupplier.forUnaryAggregateMetricDouble(
            suppliers,
            "ToStringFromAggregateMetricDoubleEvaluator[field=" + read + "]",
            DataType.KEYWORD,
            agg -> new BytesRef(EsqlDataTypeConverter.aggregateMetricDoubleLiteralToString(agg)),
            List.of()
        );
        TestCaseSupplier.forUnaryExponentialHistogram(
            suppliers,
            "ToStringFromExponentialHistogramEvaluator[histogram=" + read + "]",
            DataType.KEYWORD,
            eh -> new BytesRef(EsqlDataTypeConverter.exponentialHistogramToString(eh)),
            List.of()
        );
        ExponentialHistogram largeExponentialHistogram = buildDummyHistogram(100_001);
        suppliers.add(
            new TestCaseSupplier(
                "<too many exponential histogram buckets>",
                List.of(DataType.EXPONENTIAL_HISTOGRAM),
                () -> new TestCaseSupplier.TestCase(
                    List.of(
                        new TestCaseSupplier.TypedData(
                            new WriteableExponentialHistogram(largeExponentialHistogram),
                            DataType.EXPONENTIAL_HISTOGRAM,
                            "large exponential histogram"
                        )
                    ),
                    "ToStringFromExponentialHistogramEvaluator[histogram=" + read + "]",
                    DataType.KEYWORD,
                    is(nullValue())
                ).withWarning("Line 1:1: evaluation of [source] failed, treating result as null. Only first 20 failures recorded.")
                    .withWarning(
                        "Line 1:1: java.lang.IllegalArgumentException: Exponential histogram is too big to be converted to a string"
                    )
            )
        );

        TestCaseSupplier.forUnaryDateRange(
            suppliers,
            "ToStringFromDateRangeEvaluator[field=" + read + "]",
            DataType.KEYWORD,
            dr -> new BytesRef(EsqlDataTypeConverter.dateRangeToString(dr)),
            List.of()
        );
        TestCaseSupplier.forUnaryHistogram(
            suppliers,
            "ToStringFromHistogramEvaluator[histogram=" + read + "]",
            DataType.KEYWORD,
            h -> new BytesRef(EsqlDataTypeConverter.histogramToString(h)),
            List.of()
        );
        //doesn't matter if it's not an actual encoded histogram, as we should never get to the decoding step
        BytesRef largeTDigest = new BytesRef(new byte[3 * 1024*1024]);
        suppliers.add(
            new TestCaseSupplier(
                "<too large histograms>",
                List.of(DataType.HISTOGRAM),
                () -> new TestCaseSupplier.TestCase(
                    List.of(
                        new TestCaseSupplier.TypedData(
                            largeTDigest,
                            DataType.HISTOGRAM,
                            "large histogram"
                        )
                    ),
                    "ToStringFromHistogramEvaluator[histogram=" + read + "]",
                    DataType.KEYWORD,
                    is(nullValue())
                ).withWarning("Line 1:1: evaluation of [source] failed, treating result as null. Only first 20 failures recorded.")
                    .withWarning(
                        "Line 1:1: java.lang.IllegalArgumentException: Histogram length is greater than 2MB"
                    )
            )
        );
        return parameterSuppliersFromTypedDataWithDefaultChecks(true, suppliers);
    }

    private static ExponentialHistogram buildDummyHistogram(int bucketCount) {
        final ExponentialHistogram tooLarge;
        try (ExponentialHistogramBuilder builder = ExponentialHistogram.builder((byte) 0, ExponentialHistogramCircuitBreaker.noop())) {
            for (int i = 0; i < bucketCount; i++) {
                // indices must be unique to count as distinct buckets
                if (i % 2 == 0) {
                    builder.setPositiveBucket(i, 1L);
                } else {
                    builder.setNegativeBucket(i, 1L);
                }
            }
            tooLarge = builder.build();
        }
        return tooLarge;
    }

    @Override
    protected Expression build(Source source, List<Expression> args) {
        return new ToString(source, args.get(0));
    }
}
