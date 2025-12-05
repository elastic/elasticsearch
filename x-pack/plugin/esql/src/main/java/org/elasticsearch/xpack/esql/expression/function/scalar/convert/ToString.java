/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.expression.function.scalar.convert;

import org.apache.lucene.util.BytesRef;
import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.time.DateFormatter;
import org.elasticsearch.compute.ann.ConvertEvaluator;
import org.elasticsearch.compute.ann.Fixed;
import org.elasticsearch.exponentialhistogram.ExponentialHistogram;
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.tree.NodeInfo;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.evaluator.mapper.EvaluatorMapper;
import org.elasticsearch.xpack.esql.expression.function.ConfigurationFunction;
import org.elasticsearch.xpack.esql.expression.function.Example;
import org.elasticsearch.xpack.esql.expression.function.FunctionInfo;
import org.elasticsearch.xpack.esql.expression.function.Param;
import org.elasticsearch.xpack.esql.io.stream.PlanStreamInput;
import org.elasticsearch.xpack.esql.session.Configuration;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

import static org.elasticsearch.xpack.esql.core.type.DataType.AGGREGATE_METRIC_DOUBLE;
import static org.elasticsearch.xpack.esql.core.type.DataType.BOOLEAN;
import static org.elasticsearch.xpack.esql.core.type.DataType.CARTESIAN_POINT;
import static org.elasticsearch.xpack.esql.core.type.DataType.CARTESIAN_SHAPE;
import static org.elasticsearch.xpack.esql.core.type.DataType.DATETIME;
import static org.elasticsearch.xpack.esql.core.type.DataType.DATE_NANOS;
import static org.elasticsearch.xpack.esql.core.type.DataType.DENSE_VECTOR;
import static org.elasticsearch.xpack.esql.core.type.DataType.DOUBLE;
import static org.elasticsearch.xpack.esql.core.type.DataType.EXPONENTIAL_HISTOGRAM;
import static org.elasticsearch.xpack.esql.core.type.DataType.GEOHASH;
import static org.elasticsearch.xpack.esql.core.type.DataType.GEOHEX;
import static org.elasticsearch.xpack.esql.core.type.DataType.GEOTILE;
import static org.elasticsearch.xpack.esql.core.type.DataType.GEO_POINT;
import static org.elasticsearch.xpack.esql.core.type.DataType.GEO_SHAPE;
import static org.elasticsearch.xpack.esql.core.type.DataType.INTEGER;
import static org.elasticsearch.xpack.esql.core.type.DataType.IP;
import static org.elasticsearch.xpack.esql.core.type.DataType.KEYWORD;
import static org.elasticsearch.xpack.esql.core.type.DataType.LONG;
import static org.elasticsearch.xpack.esql.core.type.DataType.TEXT;
import static org.elasticsearch.xpack.esql.core.type.DataType.UNSIGNED_LONG;
import static org.elasticsearch.xpack.esql.core.type.DataType.VERSION;
import static org.elasticsearch.xpack.esql.type.EsqlDataTypeConverter.DEFAULT_DATE_NANOS_FORMATTER;
import static org.elasticsearch.xpack.esql.type.EsqlDataTypeConverter.DEFAULT_DATE_TIME_FORMATTER;
import static org.elasticsearch.xpack.esql.type.EsqlDataTypeConverter.dateTimeToString;
import static org.elasticsearch.xpack.esql.type.EsqlDataTypeConverter.exponentialHistogramToString;
import static org.elasticsearch.xpack.esql.type.EsqlDataTypeConverter.geoGridToString;
import static org.elasticsearch.xpack.esql.type.EsqlDataTypeConverter.ipToString;
import static org.elasticsearch.xpack.esql.type.EsqlDataTypeConverter.nanoTimeToString;
import static org.elasticsearch.xpack.esql.type.EsqlDataTypeConverter.numericBooleanToString;
import static org.elasticsearch.xpack.esql.type.EsqlDataTypeConverter.spatialToString;
import static org.elasticsearch.xpack.esql.type.EsqlDataTypeConverter.unsignedLongToString;
import static org.elasticsearch.xpack.esql.type.EsqlDataTypeConverter.versionToString;

public class ToString extends AbstractConvertFunction implements EvaluatorMapper, ConfigurationFunction {
    public static final NamedWriteableRegistry.Entry ENTRY = new NamedWriteableRegistry.Entry(Expression.class, "ToString", ToString::new);

    private static final Map<DataType, BuildFactory> STATIC_EVALUATORS = Map.ofEntries(
        Map.entry(KEYWORD, (source, fieldEval) -> fieldEval),
        Map.entry(BOOLEAN, ToStringFromBooleanEvaluator.Factory::new),
        Map.entry(IP, ToStringFromIPEvaluator.Factory::new),
        Map.entry(DENSE_VECTOR, ToStringFromFloatEvaluator.Factory::new),
        Map.entry(DOUBLE, ToStringFromDoubleEvaluator.Factory::new),
        Map.entry(LONG, ToStringFromLongEvaluator.Factory::new),
        Map.entry(INTEGER, ToStringFromIntEvaluator.Factory::new),
        Map.entry(TEXT, (source, fieldEval) -> fieldEval),
        Map.entry(VERSION, ToStringFromVersionEvaluator.Factory::new),
        Map.entry(UNSIGNED_LONG, ToStringFromUnsignedLongEvaluator.Factory::new),
        Map.entry(GEO_POINT, ToStringFromGeoPointEvaluator.Factory::new),
        Map.entry(CARTESIAN_POINT, ToStringFromCartesianPointEvaluator.Factory::new),
        Map.entry(CARTESIAN_SHAPE, ToStringFromCartesianShapeEvaluator.Factory::new),
        Map.entry(GEO_SHAPE, ToStringFromGeoShapeEvaluator.Factory::new),
        Map.entry(GEOHASH, (source, fieldEval) -> new ToStringFromGeoGridEvaluator.Factory(source, fieldEval, GEOHASH)),
        Map.entry(GEOTILE, (source, fieldEval) -> new ToStringFromGeoGridEvaluator.Factory(source, fieldEval, GEOTILE)),
        Map.entry(GEOHEX, (source, fieldEval) -> new ToStringFromGeoGridEvaluator.Factory(source, fieldEval, GEOHEX)),
        Map.entry(AGGREGATE_METRIC_DOUBLE, ToStringFromAggregateMetricDoubleEvaluator.Factory::new),
        Map.entry(EXPONENTIAL_HISTOGRAM, ToStringFromExponentialHistogramEvaluator.Factory::new),

        // Evaluators dynamically created in #factories()
        Map.entry(DATETIME, (source, fieldEval) -> null),
        Map.entry(DATE_NANOS, (source, fieldEval) -> null)
    );

    private Map<DataType, BuildFactory> lazyEvaluators = null;

    private final Configuration configuration;

    @FunctionInfo(
        returnType = "keyword",
        description = "Converts an input value into a string.",
        examples = {
            @Example(file = "string", tag = "to_string"),
            @Example(description = "It also works fine on multivalued fields:", file = "string", tag = "to_string_multivalue") }
    )
    public ToString(
        Source source,
        @Param(
            name = "field",
            type = {
                "aggregate_metric_double",
                "boolean",
                "cartesian_point",
                "cartesian_shape",
                "date",
                "date_nanos",
                "dense_vector",
                "double",
                "geo_point",
                "geo_shape",
                "geohash",
                "geotile",
                "geohex",
                "integer",
                "ip",
                "keyword",
                "long",
                "text",
                "unsigned_long",
                "version",
                "exponential_histogram" },
            description = "Input value. The input can be a single- or multi-valued column or an expression."
        ) Expression v,
        Configuration configuration
    ) {
        super(source, v);
        this.configuration = configuration;
    }

    private ToString(StreamInput in) throws IOException {
        super(in);
        this.configuration = ((PlanStreamInput) in).configuration();
    }

    @Override
    public String getWriteableName() {
        return ENTRY.name;
    }

    @Override
    protected Map<DataType, BuildFactory> factories() {
        if (lazyEvaluators == null) {
            Map<DataType, BuildFactory> evaluators = new HashMap<>(STATIC_EVALUATORS);
            evaluators.putAll(
                Map.ofEntries(
                    Map.entry(
                        DATETIME,
                        (source, fieldEval) -> new ToStringFromDatetimeEvaluator.Factory(
                            source,
                            fieldEval,
                            DEFAULT_DATE_TIME_FORMATTER.withZone(configuration.zoneId())
                        )
                    ),
                    Map.entry(
                        DATE_NANOS,
                        (source, fieldEval) -> new ToStringFromDateNanosEvaluator.Factory(
                            source,
                            fieldEval,
                            DEFAULT_DATE_NANOS_FORMATTER.withZone(configuration.zoneId())
                        )
                    )
                )
            );
            lazyEvaluators = evaluators;
        }
        return lazyEvaluators;
    }

    @Override
    public DataType dataType() {
        return KEYWORD;
    }

    @Override
    public Expression replaceChildren(List<Expression> newChildren) {
        return new ToString(source(), newChildren.get(0), configuration);
    }

    @Override
    protected NodeInfo<? extends Expression> info() {
        return NodeInfo.create(this, ToString::new, field(), configuration);
    }

    @ConvertEvaluator(extraName = "FromBoolean")
    static BytesRef fromBoolean(boolean bool) {
        return numericBooleanToString(bool);
    }

    @ConvertEvaluator(extraName = "FromIP")
    static BytesRef fromIP(BytesRef ip) {
        return new BytesRef(ipToString(ip));
    }

    @ConvertEvaluator(extraName = "FromDatetime")
    static BytesRef fromDatetime(long datetime, @Fixed DateFormatter formatter) {
        return new BytesRef(dateTimeToString(datetime, formatter));
    }

    @ConvertEvaluator(extraName = "FromDateNanos")
    static BytesRef fromDateNanos(long datetime, @Fixed DateFormatter formatter) {
        return new BytesRef(nanoTimeToString(datetime, formatter));
    }

    @ConvertEvaluator(extraName = "FromDouble")
    static BytesRef fromDouble(double dbl) {
        return numericBooleanToString(dbl);
    }

    @ConvertEvaluator(extraName = "FromFloat")
    static BytesRef fromFloat(float flt) {
        return numericBooleanToString(flt);
    }

    @ConvertEvaluator(extraName = "FromLong")
    static BytesRef fromDouble(long lng) {
        return numericBooleanToString(lng);
    }

    @ConvertEvaluator(extraName = "FromInt")
    static BytesRef fromDouble(int integer) {
        return numericBooleanToString(integer);
    }

    @ConvertEvaluator(extraName = "FromVersion")
    static BytesRef fromVersion(BytesRef version) {
        return new BytesRef(versionToString(version));
    }

    @ConvertEvaluator(extraName = "FromUnsignedLong")
    static BytesRef fromUnsignedLong(long lng) {
        return unsignedLongToString(lng);
    }

    @ConvertEvaluator(extraName = "FromGeoPoint")
    static BytesRef fromGeoPoint(BytesRef wkb) {
        return new BytesRef(spatialToString(wkb));
    }

    @ConvertEvaluator(extraName = "FromCartesianPoint")
    static BytesRef fromCartesianPoint(BytesRef wkb) {
        return new BytesRef(spatialToString(wkb));
    }

    @ConvertEvaluator(extraName = "FromCartesianShape")
    static BytesRef fromCartesianShape(BytesRef wkb) {
        return new BytesRef(spatialToString(wkb));
    }

    @ConvertEvaluator(extraName = "FromGeoShape")
    static BytesRef fromGeoShape(BytesRef wkb) {
        return new BytesRef(spatialToString(wkb));
    }

    @ConvertEvaluator(extraName = "FromGeoGrid")
    static BytesRef fromGeoGrid(long gridId, @Fixed DataType dataType) {
        return new BytesRef(geoGridToString(gridId, dataType));
    }

    @ConvertEvaluator(extraName = "FromExponentialHistogram")
    static BytesRef fromExponentialHistogram(ExponentialHistogram histogram) {
        return new BytesRef(exponentialHistogramToString(histogram));
    }

    @Override
    public int hashCode() {
        return Objects.hash(getClass(), children(), configuration);
    }

    @Override
    public boolean equals(Object obj) {
        if (super.equals(obj) == false) {
            return false;
        }
        ToString other = (ToString) obj;

        return configuration.equals(other.configuration);
    }
}
