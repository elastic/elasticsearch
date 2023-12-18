/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.expression.function.scalar.convert;

import org.apache.lucene.util.BytesRef;
import org.elasticsearch.compute.ann.ConvertEvaluator;
import org.elasticsearch.search.DocValueFormat;
import org.elasticsearch.xpack.esql.evaluator.mapper.EvaluatorMapper;
import org.elasticsearch.xpack.esql.expression.function.FunctionInfo;
import org.elasticsearch.xpack.esql.expression.function.Param;
import org.elasticsearch.xpack.ql.expression.Expression;
import org.elasticsearch.xpack.ql.tree.NodeInfo;
import org.elasticsearch.xpack.ql.tree.Source;
import org.elasticsearch.xpack.ql.type.DataType;
import org.elasticsearch.xpack.versionfield.Version;

import java.util.List;
import java.util.Map;

import static org.elasticsearch.xpack.esql.type.EsqlDataTypes.CARTESIAN_POINT;
import static org.elasticsearch.xpack.esql.type.EsqlDataTypes.GEO_POINT;
import static org.elasticsearch.xpack.ql.type.DataTypes.BOOLEAN;
import static org.elasticsearch.xpack.ql.type.DataTypes.DATETIME;
import static org.elasticsearch.xpack.ql.type.DataTypes.DOUBLE;
import static org.elasticsearch.xpack.ql.type.DataTypes.INTEGER;
import static org.elasticsearch.xpack.ql.type.DataTypes.IP;
import static org.elasticsearch.xpack.ql.type.DataTypes.KEYWORD;
import static org.elasticsearch.xpack.ql.type.DataTypes.LONG;
import static org.elasticsearch.xpack.ql.type.DataTypes.TEXT;
import static org.elasticsearch.xpack.ql.type.DataTypes.UNSIGNED_LONG;
import static org.elasticsearch.xpack.ql.type.DataTypes.VERSION;
import static org.elasticsearch.xpack.ql.util.DateUtils.UTC_DATE_TIME_FORMATTER;
import static org.elasticsearch.xpack.ql.util.NumericUtils.unsignedLongAsNumber;
import static org.elasticsearch.xpack.ql.util.SpatialCoordinateTypes.CARTESIAN;
import static org.elasticsearch.xpack.ql.util.SpatialCoordinateTypes.GEO;

public class ToString extends AbstractConvertFunction implements EvaluatorMapper {

    private static final Map<DataType, BuildFactory> EVALUATORS = Map.ofEntries(
        Map.entry(KEYWORD, (fieldEval, source) -> fieldEval),
        Map.entry(BOOLEAN, ToStringFromBooleanEvaluator.Factory::new),
        Map.entry(DATETIME, ToStringFromDatetimeEvaluator.Factory::new),
        Map.entry(IP, ToStringFromIPEvaluator.Factory::new),
        Map.entry(DOUBLE, ToStringFromDoubleEvaluator.Factory::new),
        Map.entry(LONG, ToStringFromLongEvaluator.Factory::new),
        Map.entry(INTEGER, ToStringFromIntEvaluator.Factory::new),
        Map.entry(TEXT, (fieldEval, source) -> fieldEval),
        Map.entry(VERSION, ToStringFromVersionEvaluator.Factory::new),
        Map.entry(UNSIGNED_LONG, ToStringFromUnsignedLongEvaluator.Factory::new),
        Map.entry(GEO_POINT, ToStringFromGeoPointEvaluator.Factory::new),
        Map.entry(CARTESIAN_POINT, ToStringFromCartesianPointEvaluator.Factory::new)
    );

    @FunctionInfo(returnType = "keyword")
    public ToString(
        Source source,
        @Param(
            name = "v",
            type = {
                "unsigned_long",
                "date",
                "boolean",
                "double",
                "ip",
                "text",
                "integer",
                "keyword",
                "version",
                "long",
                "geo_point",
                "cartesian_point" }
        ) Expression v
    ) {
        super(source, v);
    }

    @Override
    protected Map<DataType, BuildFactory> factories() {
        return EVALUATORS;
    }

    @Override
    public DataType dataType() {
        return KEYWORD;
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
        return new BytesRef(String.valueOf(bool));
    }

    @ConvertEvaluator(extraName = "FromIP")
    static BytesRef fromIP(BytesRef ip) {
        return new BytesRef(DocValueFormat.IP.format(ip));
    }

    @ConvertEvaluator(extraName = "FromDatetime")
    static BytesRef fromDatetime(long datetime) {
        return new BytesRef(UTC_DATE_TIME_FORMATTER.formatMillis(datetime));
    }

    @ConvertEvaluator(extraName = "FromDouble")
    static BytesRef fromDouble(double dbl) {
        return new BytesRef(String.valueOf(dbl));
    }

    @ConvertEvaluator(extraName = "FromLong")
    static BytesRef fromDouble(long lng) {
        return new BytesRef(String.valueOf(lng));
    }

    @ConvertEvaluator(extraName = "FromInt")
    static BytesRef fromDouble(int integer) {
        return new BytesRef(String.valueOf(integer));
    }

    @ConvertEvaluator(extraName = "FromVersion")
    static BytesRef fromVersion(BytesRef version) {
        return new BytesRef(new Version(version).toString());
    }

    @ConvertEvaluator(extraName = "FromUnsignedLong")
    static BytesRef fromUnsignedLong(long lng) {
        return new BytesRef(unsignedLongAsNumber(lng).toString());
    }

    @ConvertEvaluator(extraName = "FromGeoPoint")
    static BytesRef fromGeoPoint(long point) {
        return new BytesRef(GEO.pointAsString(GEO.longAsPoint(point)));
    }

    @ConvertEvaluator(extraName = "FromCartesianPoint")
    static BytesRef fromCartesianPoint(long point) {
        return new BytesRef(CARTESIAN.pointAsString(CARTESIAN.longAsPoint(point)));
    }
}
