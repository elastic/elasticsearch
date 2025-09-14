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
import org.elasticsearch.compute.ann.ConvertEvaluator;
import org.elasticsearch.compute.ann.Fixed;
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.tree.NodeInfo;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.evaluator.mapper.EvaluatorMapper;
import org.elasticsearch.xpack.esql.expression.function.Example;
import org.elasticsearch.xpack.esql.expression.function.FunctionInfo;
import org.elasticsearch.xpack.esql.expression.function.Param;

import java.io.IOException;
import java.util.List;
import java.util.Map;

import static org.elasticsearch.xpack.esql.core.type.AtomType.AGGREGATE_METRIC_DOUBLE;
import static org.elasticsearch.xpack.esql.core.type.AtomType.BOOLEAN;
import static org.elasticsearch.xpack.esql.core.type.AtomType.CARTESIAN_POINT;
import static org.elasticsearch.xpack.esql.core.type.AtomType.CARTESIAN_SHAPE;
import static org.elasticsearch.xpack.esql.core.type.AtomType.DATETIME;
import static org.elasticsearch.xpack.esql.core.type.AtomType.DATE_NANOS;
import static org.elasticsearch.xpack.esql.core.type.AtomType.DOUBLE;
import static org.elasticsearch.xpack.esql.core.type.AtomType.GEOHASH;
import static org.elasticsearch.xpack.esql.core.type.AtomType.GEOHEX;
import static org.elasticsearch.xpack.esql.core.type.AtomType.GEOTILE;
import static org.elasticsearch.xpack.esql.core.type.AtomType.GEO_POINT;
import static org.elasticsearch.xpack.esql.core.type.AtomType.GEO_SHAPE;
import static org.elasticsearch.xpack.esql.core.type.AtomType.INTEGER;
import static org.elasticsearch.xpack.esql.core.type.AtomType.IP;
import static org.elasticsearch.xpack.esql.core.type.AtomType.KEYWORD;
import static org.elasticsearch.xpack.esql.core.type.AtomType.LONG;
import static org.elasticsearch.xpack.esql.core.type.AtomType.TEXT;
import static org.elasticsearch.xpack.esql.core.type.AtomType.UNSIGNED_LONG;
import static org.elasticsearch.xpack.esql.core.type.AtomType.VERSION;
import static org.elasticsearch.xpack.esql.type.EsqlDataTypeConverter.dateTimeToString;
import static org.elasticsearch.xpack.esql.type.EsqlDataTypeConverter.geoGridToString;
import static org.elasticsearch.xpack.esql.type.EsqlDataTypeConverter.ipToString;
import static org.elasticsearch.xpack.esql.type.EsqlDataTypeConverter.nanoTimeToString;
import static org.elasticsearch.xpack.esql.type.EsqlDataTypeConverter.numericBooleanToString;
import static org.elasticsearch.xpack.esql.type.EsqlDataTypeConverter.spatialToString;
import static org.elasticsearch.xpack.esql.type.EsqlDataTypeConverter.unsignedLongToString;
import static org.elasticsearch.xpack.esql.type.EsqlDataTypeConverter.versionToString;

public class ToString extends AbstractConvertFunction implements EvaluatorMapper {
    public static final NamedWriteableRegistry.Entry ENTRY = new NamedWriteableRegistry.Entry(Expression.class, "ToString", ToString::new);

    private static final Map<DataType, BuildFactory> EVALUATORS = Map.ofEntries(
        Map.entry(KEYWORD.type(), (source, fieldEval) -> fieldEval),
        Map.entry(BOOLEAN.type(), ToStringFromBooleanEvaluator.Factory::new),
        Map.entry(DATETIME.type(), ToStringFromDatetimeEvaluator.Factory::new),
        Map.entry(DATE_NANOS.type(), ToStringFromDateNanosEvaluator.Factory::new),
        Map.entry(IP.type(), ToStringFromIPEvaluator.Factory::new),
        Map.entry(DOUBLE.type(), ToStringFromDoubleEvaluator.Factory::new),
        Map.entry(LONG.type(), ToStringFromLongEvaluator.Factory::new),
        Map.entry(INTEGER.type(), ToStringFromIntEvaluator.Factory::new),
        Map.entry(TEXT.type(), (source, fieldEval) -> fieldEval),
        Map.entry(VERSION.type(), ToStringFromVersionEvaluator.Factory::new),
        Map.entry(UNSIGNED_LONG.type(), ToStringFromUnsignedLongEvaluator.Factory::new),
        Map.entry(GEO_POINT.type(), ToStringFromGeoPointEvaluator.Factory::new),
        Map.entry(CARTESIAN_POINT.type(), ToStringFromCartesianPointEvaluator.Factory::new),
        Map.entry(CARTESIAN_SHAPE.type(), ToStringFromCartesianShapeEvaluator.Factory::new),
        Map.entry(GEO_SHAPE.type(), ToStringFromGeoShapeEvaluator.Factory::new),
        Map.entry(GEOHASH.type(), (source, fieldEval) -> new ToStringFromGeoGridEvaluator.Factory(source, fieldEval, GEOHASH.type())),
        Map.entry(GEOTILE.type(), (source, fieldEval) -> new ToStringFromGeoGridEvaluator.Factory(source, fieldEval, GEOTILE.type())),
        Map.entry(GEOHEX.type(), (source, fieldEval) -> new ToStringFromGeoGridEvaluator.Factory(source, fieldEval, GEOHEX.type())),
        Map.entry(AGGREGATE_METRIC_DOUBLE.type(), ToStringFromAggregateMetricDoubleEvaluator.Factory::new)
    );

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
                "version" },
            description = "Input value. The input can be a single- or multi-valued column or an expression."
        ) Expression v
    ) {
        super(source, v);
    }

    private ToString(StreamInput in) throws IOException {
        super(in);
    }

    @Override
    public String getWriteableName() {
        return ENTRY.name;
    }

    @Override
    protected Map<DataType, BuildFactory> factories() {
        return EVALUATORS;
    }

    @Override
    public DataType dataType() {
        return KEYWORD.type();
    }

    @Override
    public Expression replaceChildren(List<Expression> newChildren) {
        return new ToString(source(), newChildren.get(0));
    }

    @Override
    protected NodeInfo<? extends Expression> info() {
        return NodeInfo.create(this, ToString::new, field());
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
    static BytesRef fromDatetime(long datetime) {
        return new BytesRef(dateTimeToString(datetime));
    }

    @ConvertEvaluator(extraName = "FromDateNanos")
    static BytesRef fromDateNanos(long datetime) {
        return new BytesRef(nanoTimeToString(datetime));
    }

    @ConvertEvaluator(extraName = "FromDouble")
    static BytesRef fromDouble(double dbl) {
        return numericBooleanToString(dbl);
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
}
