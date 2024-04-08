/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.planner;

import org.elasticsearch.compute.aggregation.IntermediateStateDesc;
import org.elasticsearch.compute.data.ElementType;
import org.elasticsearch.core.Tuple;
import org.elasticsearch.xpack.esql.EsqlIllegalArgumentException;
import org.elasticsearch.xpack.esql.expression.function.aggregate.Count;
import org.elasticsearch.xpack.esql.expression.function.aggregate.CountDistinct;
import org.elasticsearch.xpack.esql.expression.function.aggregate.Max;
import org.elasticsearch.xpack.esql.expression.function.aggregate.MedianAbsoluteDeviation;
import org.elasticsearch.xpack.esql.expression.function.aggregate.Min;
import org.elasticsearch.xpack.esql.expression.function.aggregate.NumericAggregate;
import org.elasticsearch.xpack.esql.expression.function.aggregate.Percentile;
import org.elasticsearch.xpack.esql.expression.function.aggregate.SpatialCentroid;
import org.elasticsearch.xpack.esql.expression.function.aggregate.Sum;
import org.elasticsearch.xpack.esql.expression.function.aggregate.Values;
import org.elasticsearch.xpack.ql.expression.Alias;
import org.elasticsearch.xpack.ql.expression.AttributeMap;
import org.elasticsearch.xpack.ql.expression.Expression;
import org.elasticsearch.xpack.ql.expression.FieldAttribute;
import org.elasticsearch.xpack.ql.expression.MetadataAttribute;
import org.elasticsearch.xpack.ql.expression.NamedExpression;
import org.elasticsearch.xpack.ql.expression.ReferenceAttribute;
import org.elasticsearch.xpack.ql.expression.function.Function;
import org.elasticsearch.xpack.ql.expression.function.aggregate.AggregateFunction;
import org.elasticsearch.xpack.ql.expression.function.aggregate.SpatialAggregateFunction;
import org.elasticsearch.xpack.ql.tree.Source;
import org.elasticsearch.xpack.ql.type.DataType;
import org.elasticsearch.xpack.ql.type.DataTypes;

import java.lang.invoke.MethodHandle;
import java.lang.invoke.MethodHandles;
import java.lang.invoke.MethodType;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.elasticsearch.xpack.esql.type.EsqlDataTypes.CARTESIAN_POINT;
import static org.elasticsearch.xpack.esql.type.EsqlDataTypes.GEO_POINT;

public class AggregateMapper {

    static final List<String> NUMERIC = List.of("Int", "Long", "Double");
    static final List<String> SPATIAL = List.of("GeoPoint", "CartesianPoint");

    /** List of all mappable ESQL agg functions (excludes surrogates like AVG = SUM/COUNT). */
    static final List<? extends Class<? extends Function>> AGG_FUNCTIONS = List.of(
        Count.class,
        CountDistinct.class,
        Max.class,
        MedianAbsoluteDeviation.class,
        Min.class,
        Percentile.class,
        SpatialCentroid.class,
        Sum.class,
        Values.class
    );

    /** Record of agg Class, type, and grouping (or non-grouping). */
    record AggDef(Class<?> aggClazz, String type, String extra, boolean grouping) {}

    /** Map of AggDef types to intermediate named expressions. */
    private final Map<AggDef, List<IntermediateStateDesc>> mapper;

    /** Cache of aggregates to intermediate expressions. */
    private final HashMap<Expression, List<? extends NamedExpression>> cache = new HashMap<>();

    AggregateMapper() {
        this(AGG_FUNCTIONS);
    }

    AggregateMapper(List<? extends Class<? extends Function>> aggregateFunctionClasses) {
        mapper = aggregateFunctionClasses.stream()
            .flatMap(AggregateMapper::typeAndNames)
            .flatMap(AggregateMapper::groupingAndNonGrouping)
            .collect(Collectors.toUnmodifiableMap(aggDef -> aggDef, AggregateMapper::lookupIntermediateState));
    }

    public List<? extends NamedExpression> mapNonGrouping(List<? extends Expression> aggregates) {
        return doMapping(aggregates, false);
    }

    public List<? extends NamedExpression> mapNonGrouping(Expression aggregate) {
        return map(aggregate, false).toList();
    }

    public List<? extends NamedExpression> mapGrouping(List<? extends Expression> aggregates) {
        return doMapping(aggregates, true);
    }

    private List<? extends NamedExpression> doMapping(List<? extends Expression> aggregates, boolean grouping) {
        AttributeMap<NamedExpression> attrToExpressions = new AttributeMap<>();
        aggregates.stream().flatMap(agg -> map(agg, grouping)).forEach(ne -> attrToExpressions.put(ne.toAttribute(), ne));
        return attrToExpressions.values().stream().toList();
    }

    public List<? extends NamedExpression> mapGrouping(Expression aggregate) {
        return map(aggregate, true).toList();
    }

    private Stream<? extends NamedExpression> map(Expression aggregate, boolean grouping) {
        aggregate = Alias.unwrap(aggregate);
        return cache.computeIfAbsent(aggregate, aggKey -> computeEntryForAgg(aggKey, grouping)).stream();
    }

    private List<? extends NamedExpression> computeEntryForAgg(Expression aggregate, boolean grouping) {
        var aggDef = aggDefOrNull(aggregate, grouping);
        if (aggDef != null) {
            var is = getNonNull(aggDef);
            var exp = isToNE(is).toList();
            return exp;
        }
        if (aggregate instanceof FieldAttribute || aggregate instanceof MetadataAttribute || aggregate instanceof ReferenceAttribute) {
            // This condition is a little pedantic, but do we expected other expressions here? if so, then add them
            return List.of();
        } else {
            throw new EsqlIllegalArgumentException("unknown agg: " + aggregate.getClass() + ": " + aggregate);
        }
    }

    /** Gets the agg from the mapper - wrapper around map::get for more informative failure.*/
    private List<IntermediateStateDesc> getNonNull(AggDef aggDef) {
        var l = mapper.get(aggDef);
        if (l == null) {
            throw new EsqlIllegalArgumentException("Cannot find intermediate state for: " + aggDef);
        }
        return l;
    }

    private static Stream<Tuple<Class<?>, Tuple<String, String>>> typeAndNames(Class<?> clazz) {
        List<String> types;
        List<String> extraConfigs = List.of("");
        if (NumericAggregate.class.isAssignableFrom(clazz)) {
            types = NUMERIC;
        } else if (clazz == Count.class) {
            types = List.of(""); // no extra type distinction
        } else if (SpatialAggregateFunction.class.isAssignableFrom(clazz)) {
            types = SPATIAL;
            extraConfigs = List.of("SourceValues", "DocValues");
        } else if (Values.class.isAssignableFrom(clazz)) {
            // TODO can't we figure this out from the function itself?
            types = List.of("Int", "Long", "Double", "Boolean", "BytesRef");
        } else {
            assert clazz == CountDistinct.class : "Expected CountDistinct, got: " + clazz;
            types = Stream.concat(NUMERIC.stream(), Stream.of("Boolean", "BytesRef")).toList();
        }
        return combinations(types, extraConfigs).map(combo -> new Tuple<>(clazz, combo));
    }

    private static Stream<Tuple<String, String>> combinations(List<String> types, List<String> extraConfigs) {
        return types.stream().flatMap(type -> extraConfigs.stream().map(config -> new Tuple<>(type, config)));
    }

    private static Stream<AggDef> groupingAndNonGrouping(Tuple<Class<?>, Tuple<String, String>> tuple) {
        return Stream.of(
            new AggDef(tuple.v1(), tuple.v2().v1(), tuple.v2().v2(), true),
            new AggDef(tuple.v1(), tuple.v2().v1(), tuple.v2().v2(), false)
        );
    }

    private static AggDef aggDefOrNull(Expression aggregate, boolean grouping) {
        if (aggregate instanceof AggregateFunction aggregateFunction) {
            return new AggDef(
                aggregateFunction.getClass(),
                dataTypeToString(aggregateFunction.field().dataType(), aggregateFunction.getClass()),
                aggregate instanceof SpatialCentroid ? "SourceValues" : "",
                grouping
            );
        }
        return null;
    }

    /** Retrieves the intermediate state description for a given class, type, and grouping. */
    private static List<IntermediateStateDesc> lookupIntermediateState(AggDef aggDef) {
        try {
            return (List<IntermediateStateDesc>) lookup(aggDef.aggClazz(), aggDef.type(), aggDef.extra(), aggDef.grouping()).invokeExact();
        } catch (Throwable t) {
            // invokeExact forces us to handle any Throwable thrown by lookup.
            throw new EsqlIllegalArgumentException(t);
        }
    }

    /** Looks up the intermediate state method for a given class, type, and grouping. */
    private static MethodHandle lookup(Class<?> clazz, String type, String extra, boolean grouping) {
        try {
            return MethodHandles.lookup()
                .findStatic(
                    Class.forName(determineAggName(clazz, type, extra, grouping)),
                    "intermediateStateDesc",
                    MethodType.methodType(List.class)
                );
        } catch (IllegalAccessException | NoSuchMethodException | ClassNotFoundException e) {
            throw new EsqlIllegalArgumentException(e);
        }
    }

    /** Determines the engines agg class name, for the given class, type, and grouping. */
    private static String determineAggName(Class<?> clazz, String type, String extra, boolean grouping) {
        StringBuilder sb = new StringBuilder();
        sb.append(determinePackageName(clazz)).append(".");
        sb.append(clazz.getSimpleName());
        sb.append(type);
        sb.append(extra);
        sb.append(grouping ? "Grouping" : "");
        sb.append("AggregatorFunction");
        return sb.toString();
    }

    /** Determines the engine agg package name, for the given class. */
    private static String determinePackageName(Class<?> clazz) {
        if (clazz.getSimpleName().startsWith("Spatial")) {
            // All spatial aggs are in the spatial sub-package
            return "org.elasticsearch.compute.aggregation.spatial";
        }
        return "org.elasticsearch.compute.aggregation";
    }

    /** Maps intermediate state description to named expressions.  */
    private static Stream<NamedExpression> isToNE(List<IntermediateStateDesc> intermediateStateDescs) {
        return intermediateStateDescs.stream().map(is -> new ReferenceAttribute(Source.EMPTY, is.name(), toDataType(is.type())));
    }

    /** Returns the data type for the engines element type. */
    // defaults to aggstate, but we'll eventually be able to remove this
    private static DataType toDataType(ElementType elementType) {
        return switch (elementType) {
            case BOOLEAN -> DataTypes.BOOLEAN;
            case BYTES_REF -> DataTypes.KEYWORD;
            case INT -> DataTypes.INTEGER;
            case LONG -> DataTypes.LONG;
            case DOUBLE -> DataTypes.DOUBLE;
            default -> throw new EsqlIllegalArgumentException("unsupported agg type: " + elementType);
        };
    }

    /** Returns the string representation for the data type. This reflects the engine's aggs naming structure. */
    private static String dataTypeToString(DataType type, Class<?> aggClass) {
        if (aggClass == Count.class) {
            return "";  // no type distinction
        }
        if (type.equals(DataTypes.BOOLEAN)) {
            return "Boolean";
        } else if (type.equals(DataTypes.INTEGER)) {
            return "Int";
        } else if (type.equals(DataTypes.LONG) || type.equals(DataTypes.DATETIME)) {
            return "Long";
        } else if (type.equals(DataTypes.DOUBLE)) {
            return "Double";
        } else if (type.equals(DataTypes.KEYWORD)
            || type.equals(DataTypes.IP)
            || type.equals(DataTypes.VERSION)
            || type.equals(DataTypes.TEXT)) {
                return "BytesRef";
            } else if (type.equals(GEO_POINT)) {
                return "GeoPoint";
            } else if (type.equals(CARTESIAN_POINT)) {
                return "CartesianPoint";
            } else {
                throw new EsqlIllegalArgumentException("illegal agg type: " + type.typeName());
            }
    }

    private static Expression unwrapAlias(Expression expression) {
        if (expression instanceof Alias alias) return alias.child();
        return expression;
    }
}
