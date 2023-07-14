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
import org.elasticsearch.xpack.esql.expression.MetadataAttribute;
import org.elasticsearch.xpack.esql.expression.SurrogateExpression;
import org.elasticsearch.xpack.esql.expression.function.aggregate.Count;
import org.elasticsearch.xpack.esql.expression.function.aggregate.CountDistinct;
import org.elasticsearch.xpack.esql.expression.function.aggregate.Max;
import org.elasticsearch.xpack.esql.expression.function.aggregate.Median;
import org.elasticsearch.xpack.esql.expression.function.aggregate.MedianAbsoluteDeviation;
import org.elasticsearch.xpack.esql.expression.function.aggregate.Min;
import org.elasticsearch.xpack.esql.expression.function.aggregate.NumericAggregate;
import org.elasticsearch.xpack.esql.expression.function.aggregate.Percentile;
import org.elasticsearch.xpack.esql.expression.function.aggregate.Sum;
import org.elasticsearch.xpack.ql.expression.Alias;
import org.elasticsearch.xpack.ql.expression.Expression;
import org.elasticsearch.xpack.ql.expression.FieldAttribute;
import org.elasticsearch.xpack.ql.expression.NamedExpression;
import org.elasticsearch.xpack.ql.expression.ReferenceAttribute;
import org.elasticsearch.xpack.ql.expression.function.Function;
import org.elasticsearch.xpack.ql.expression.function.aggregate.AggregateFunction;
import org.elasticsearch.xpack.ql.tree.Source;
import org.elasticsearch.xpack.ql.type.DataType;
import org.elasticsearch.xpack.ql.type.DataTypes;

import java.lang.invoke.MethodHandle;
import java.lang.invoke.MethodHandles;
import java.lang.invoke.MethodType;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class AggregateMapper {

    static final List<String> NUMERIC = List.of("Int", "Long", "Double");

    /** List of all ESQL agg functions. */
    static final List<? extends Class<? extends Function>> AGG_FUNCTIONS = List.of(
        Count.class,
        CountDistinct.class,
        Max.class,
        Median.class,
        MedianAbsoluteDeviation.class,
        Min.class,
        Percentile.class,
        Sum.class
    );

    /** Record of agg Class, type, and grouping (or non-grouping). */
    record AggDef(Class<?> aggClazz, String type, boolean grouping) {}

    /** Map of AggDef types to intermediate named expressions. */
    private final Map<AggDef, List<IntermediateStateDesc>> mapper;

    /** Cache of aggregates to intermediate expressions. */
    private final HashMap<Expression, List<? extends NamedExpression>> cache = new HashMap<>();

    AggregateMapper() {
        this(AGG_FUNCTIONS.stream().filter(Predicate.not(SurrogateExpression.class::isAssignableFrom)).toList());
    }

    AggregateMapper(List<? extends Class<? extends Function>> aggregateFunctionClasses) {
        mapper = aggregateFunctionClasses.stream()
            .flatMap(AggregateMapper::typeAndNames)
            .flatMap(AggregateMapper::groupingAndNonGrouping)
            .collect(Collectors.toUnmodifiableMap(aggDef -> aggDef, AggregateMapper::lookupIntermediateState));
    }

    public List<? extends NamedExpression> mapNonGrouping(List<? extends Expression> aggregates) {
        return aggregates.stream().flatMap(agg -> map(agg, false)).toList();
    }

    public List<? extends NamedExpression> mapNonGrouping(Expression aggregate) {
        return map(aggregate, false).toList();
    }

    public List<? extends NamedExpression> mapGrouping(List<? extends Expression> aggregates) {
        return aggregates.stream().flatMap(agg -> map(agg, true)).toList();
    }

    public List<? extends NamedExpression> mapGrouping(Expression aggregate) {
        return map(aggregate, true).toList();
    }

    private Stream<? extends NamedExpression> map(Expression aggregate, boolean grouping) {
        aggregate = unwrapAlias(aggregate);
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
            throw new UnsupportedOperationException("unknown: " + aggregate.getClass() + ": " + aggregate);
        }
    }

    /** Gets the agg from the mapper - wrapper around map::get for more informative failure.*/
    private List<IntermediateStateDesc> getNonNull(AggDef aggDef) {
        var l = mapper.get(aggDef);
        if (l == null) {
            throw new AssertionError("Cannot find intermediate state for: " + aggDef);
        }
        return l;
    }

    static Stream<Tuple<Class<?>, String>> typeAndNames(Class<?> clazz) {
        List<String> types;
        if (NumericAggregate.class.isAssignableFrom(clazz)) {
            types = NUMERIC;
        } else if (clazz == Count.class) {
            types = List.of(""); // no extra type distinction
        } else {
            assert clazz == CountDistinct.class : "Expected CountDistinct, got: " + clazz;
            types = Stream.concat(NUMERIC.stream(), Stream.of("Boolean", "BytesRef")).toList();
        }
        return types.stream().map(type -> new Tuple<>(clazz, type));
    }

    static Stream<AggDef> groupingAndNonGrouping(Tuple<Class<?>, String> tuple) {
        return Stream.of(new AggDef(tuple.v1(), tuple.v2(), true), new AggDef(tuple.v1(), tuple.v2(), false));
    }

    static AggDef aggDefOrNull(Expression aggregate, boolean grouping) {
        if (aggregate instanceof AggregateFunction aggregateFunction) {
            return new AggDef(
                aggregateFunction.getClass(),
                dataTypeToString(aggregateFunction.field().dataType(), aggregateFunction.getClass()),
                grouping
            );
        }
        return null;
    }

    /** Retrieves the intermediate state description for a given class, type, and grouping. */
    static List<IntermediateStateDesc> lookupIntermediateState(AggDef aggDef) {
        try {
            return (List<IntermediateStateDesc>) lookup(aggDef.aggClazz(), aggDef.type(), aggDef.grouping()).invokeExact();
        } catch (Throwable t) {
            throw new AssertionError(t);
        }
    }

    /** Looks up the intermediate state method for a given class, type, and grouping. */
    static MethodHandle lookup(Class<?> clazz, String type, boolean grouping) {
        try {
            return MethodHandles.lookup()
                .findStatic(
                    Class.forName(determineAggName(clazz, type, grouping)),
                    "intermediateStateDesc",
                    MethodType.methodType(List.class)
                );
        } catch (IllegalAccessException | NoSuchMethodException | ClassNotFoundException e) {
            throw new AssertionError(e);
        }
    }

    /** Determines the engines agg class name, for the given class, type, and grouping. */
    static String determineAggName(Class<?> clazz, String type, boolean grouping) {
        StringBuilder sb = new StringBuilder();
        sb.append("org.elasticsearch.compute.aggregation.");
        sb.append(clazz.getSimpleName());
        sb.append(type);
        sb.append(grouping ? "Grouping" : "");
        sb.append("AggregatorFunction");
        return sb.toString();
    }

    /** Maps intermediate state description to named expressions.  */
    static Stream<NamedExpression> isToNE(List<IntermediateStateDesc> intermediateStateDescs) {
        return intermediateStateDescs.stream().map(is -> new ReferenceAttribute(Source.EMPTY, is.name(), toDataType(is.type())));
    }

    /** Returns the data type for the engines element type. */
    // defaults to aggstate, but we'll eventually be able to remove this
    static DataType toDataType(ElementType elementType) {
        return switch (elementType) {
            case BOOLEAN -> DataTypes.BOOLEAN;
            case BYTES_REF -> DataTypes.BINARY;
            case INT -> DataTypes.INTEGER;
            case LONG -> DataTypes.LONG;
            case DOUBLE -> DataTypes.DOUBLE;
            default -> throw new UnsupportedOperationException("unsupported agg type: " + elementType);
        };
    }

    /** Returns the string representation for the data type. This reflects the engine's aggs naming structure. */
    static String dataTypeToString(DataType type, Class<?> aggClass) {
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
        } else if (type.equals(DataTypes.KEYWORD) || type.equals(DataTypes.IP)) {
            return "BytesRef";
        } else {
            throw new UnsupportedOperationException("unsupported agg type: " + type);
        }
    }

    static Expression unwrapAlias(Expression expression) {
        if (expression instanceof Alias alias) return alias.child();
        return expression;
    }
}
