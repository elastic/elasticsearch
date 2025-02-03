/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.planner;

import org.elasticsearch.common.Strings;
import org.elasticsearch.compute.aggregation.IntermediateStateDesc;
import org.elasticsearch.compute.data.ElementType;
import org.elasticsearch.core.Tuple;
import org.elasticsearch.xpack.esql.EsqlIllegalArgumentException;
import org.elasticsearch.xpack.esql.core.expression.Alias;
import org.elasticsearch.xpack.esql.core.expression.Attribute;
import org.elasticsearch.xpack.esql.core.expression.AttributeMap;
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.expression.FieldAttribute;
import org.elasticsearch.xpack.esql.core.expression.MetadataAttribute;
import org.elasticsearch.xpack.esql.core.expression.NamedExpression;
import org.elasticsearch.xpack.esql.core.expression.ReferenceAttribute;
import org.elasticsearch.xpack.esql.core.expression.function.Function;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.expression.function.aggregate.AggregateFunction;
import org.elasticsearch.xpack.esql.expression.function.aggregate.Count;
import org.elasticsearch.xpack.esql.expression.function.aggregate.CountDistinct;
import org.elasticsearch.xpack.esql.expression.function.aggregate.FromPartial;
import org.elasticsearch.xpack.esql.expression.function.aggregate.Max;
import org.elasticsearch.xpack.esql.expression.function.aggregate.MedianAbsoluteDeviation;
import org.elasticsearch.xpack.esql.expression.function.aggregate.Min;
import org.elasticsearch.xpack.esql.expression.function.aggregate.NumericAggregate;
import org.elasticsearch.xpack.esql.expression.function.aggregate.Percentile;
import org.elasticsearch.xpack.esql.expression.function.aggregate.Rate;
import org.elasticsearch.xpack.esql.expression.function.aggregate.SpatialAggregateFunction;
import org.elasticsearch.xpack.esql.expression.function.aggregate.SpatialCentroid;
import org.elasticsearch.xpack.esql.expression.function.aggregate.SpatialExtent;
import org.elasticsearch.xpack.esql.expression.function.aggregate.StdDev;
import org.elasticsearch.xpack.esql.expression.function.aggregate.Sum;
import org.elasticsearch.xpack.esql.expression.function.aggregate.ToPartial;
import org.elasticsearch.xpack.esql.expression.function.aggregate.Top;
import org.elasticsearch.xpack.esql.expression.function.aggregate.Values;

import java.lang.invoke.MethodHandle;
import java.lang.invoke.MethodHandles;
import java.lang.invoke.MethodType;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * Static class used to convert aggregate expressions to the named expressions that represent their intermediate state.
 * <p>
 *     At class load time, the mapper is populated with all supported aggregate functions and their intermediate state.
 * </p>
 * <p>
 *     Reflection is used to call the {@code intermediateStateDesc()}` static method of the aggregate functions,
 *     but the function classes are found based on the exising information within this class.
 * </p>
 * <p>
 *     This class must be updated when aggregations are created or updated, by adding the new aggs or types to the corresponding methods.
 * </p>
 */
final class AggregateMapper {

    private static final List<String> NUMERIC = List.of("Int", "Long", "Double");
    private static final List<String> SPATIAL_EXTRA_CONFIGS = List.of("SourceValues", "DocValues");

    /** List of all mappable ESQL agg functions (excludes surrogates like AVG = SUM/COUNT). */
    private static final List<? extends Class<? extends Function>> AGG_FUNCTIONS = List.of(
        Count.class,
        CountDistinct.class,
        Max.class,
        MedianAbsoluteDeviation.class,
        Min.class,
        Percentile.class,
        SpatialCentroid.class,
        SpatialExtent.class,
        StdDev.class,
        Sum.class,
        Values.class,
        Top.class,
        Rate.class,

        // internal function
        FromPartial.class,
        ToPartial.class
    );

    /** Record of agg Class, type, and grouping (or non-grouping). */
    private record AggDef(Class<?> aggClazz, String type, String extra, boolean grouping) {
        public AggDef withoutExtra() {
            return new AggDef(aggClazz, type, "", grouping);
        }
    }

    /** Map of AggDef types to intermediate named expressions. */
    private static final Map<AggDef, List<IntermediateStateDesc>> MAPPER = AGG_FUNCTIONS.stream()
        .flatMap(AggregateMapper::aggDefs)
        .collect(Collectors.toUnmodifiableMap(aggDef -> aggDef, AggregateMapper::lookupIntermediateState));

    /** Cache of aggregates to intermediate expressions. */
    private final HashMap<Expression, List<NamedExpression>> cache = new HashMap<>();

    public List<NamedExpression> mapNonGrouping(List<? extends NamedExpression> aggregates) {
        return doMapping(aggregates, false);
    }

    public List<NamedExpression> mapNonGrouping(NamedExpression aggregate) {
        return map(aggregate, false).toList();
    }

    public List<NamedExpression> mapGrouping(List<? extends NamedExpression> aggregates) {
        return doMapping(aggregates, true);
    }

    private List<NamedExpression> doMapping(List<? extends NamedExpression> aggregates, boolean grouping) {
        AttributeMap<NamedExpression> attrToExpressions = new AttributeMap<>();
        aggregates.stream().flatMap(ne -> map(ne, grouping)).forEach(ne -> attrToExpressions.put(ne.toAttribute(), ne));
        return attrToExpressions.values().stream().toList();
    }

    public List<NamedExpression> mapGrouping(NamedExpression aggregate) {
        return map(aggregate, true).toList();
    }

    private Stream<NamedExpression> map(NamedExpression ne, boolean grouping) {
        return cache.computeIfAbsent(Alias.unwrap(ne), aggKey -> computeEntryForAgg(ne.name(), aggKey, grouping)).stream();
    }

    private static List<NamedExpression> computeEntryForAgg(String aggAlias, Expression aggregate, boolean grouping) {
        if (aggregate instanceof AggregateFunction aggregateFunction) {
            return entryForAgg(aggAlias, aggregateFunction, grouping);
        }
        if (aggregate instanceof FieldAttribute || aggregate instanceof MetadataAttribute || aggregate instanceof ReferenceAttribute) {
            // This condition is a little pedantic, but do we expect other expressions here? if so, then add them
            return List.of();
        }
        throw new EsqlIllegalArgumentException("unknown agg: " + aggregate.getClass() + ": " + aggregate);
    }

    private static List<NamedExpression> entryForAgg(String aggAlias, AggregateFunction aggregateFunction, boolean grouping) {
        var aggDef = new AggDef(
            aggregateFunction.getClass(),
            dataTypeToString(aggregateFunction.field().dataType(), aggregateFunction.getClass()),
            aggregateFunction instanceof SpatialAggregateFunction ? "SourceValues" : "",
            grouping
        );
        var is = getNonNull(aggDef);
        return isToNE(is, aggAlias).toList();
    }

    /** Gets the agg from the mapper - wrapper around map::get for more informative failure.*/
    private static List<IntermediateStateDesc> getNonNull(AggDef aggDef) {
        var l = MAPPER.getOrDefault(aggDef, MAPPER.get(aggDef.withoutExtra()));
        if (l == null) {
            throw new EsqlIllegalArgumentException("Cannot find intermediate state for: " + aggDef);
        }
        return l;
    }

    private static Stream<AggDef> aggDefs(Class<?> clazz) {
        List<String> types;
        List<String> extraConfigs = List.of("");
        if (NumericAggregate.class.isAssignableFrom(clazz)) {
            types = NUMERIC;
        } else if (Max.class.isAssignableFrom(clazz) || Min.class.isAssignableFrom(clazz)) {
            types = List.of("Boolean", "Int", "Long", "Double", "Ip", "BytesRef");
        } else if (clazz == Count.class) {
            types = List.of(""); // no extra type distinction
        } else if (clazz == SpatialCentroid.class) {
            types = List.of("GeoPoint", "CartesianPoint");
            extraConfigs = SPATIAL_EXTRA_CONFIGS;
        } else if (clazz == SpatialExtent.class) {
            types = List.of("GeoPoint", "CartesianPoint", "GeoShape", "CartesianShape");
            extraConfigs = SPATIAL_EXTRA_CONFIGS;
        } else if (Values.class.isAssignableFrom(clazz)) {
            // TODO can't we figure this out from the function itself?
            types = List.of("Int", "Long", "Double", "Boolean", "BytesRef");
        } else if (Top.class.isAssignableFrom(clazz)) {
            types = List.of("Boolean", "Int", "Long", "Double", "Ip", "BytesRef");
        } else if (Rate.class.isAssignableFrom(clazz) || StdDev.class.isAssignableFrom(clazz)) {
            types = List.of("Int", "Long", "Double");
        } else if (FromPartial.class.isAssignableFrom(clazz) || ToPartial.class.isAssignableFrom(clazz)) {
            types = List.of(""); // no type
        } else if (CountDistinct.class.isAssignableFrom(clazz)) {
            types = Stream.concat(NUMERIC.stream(), Stream.of("Boolean", "BytesRef")).toList();
        } else {
            assert false : "unknown aggregate type " + clazz;
            throw new IllegalArgumentException("unknown aggregate type " + clazz);
        }
        return combinations(types, extraConfigs).flatMap(typeAndExtraConfig -> {
            var type = typeAndExtraConfig.v1();
            var extra = typeAndExtraConfig.v2();

            if (clazz.isAssignableFrom(Rate.class)) {
                // rate doesn't support non-grouping aggregations
                return Stream.of(new AggDef(clazz, type, extra, true));
            } else if (Objects.equals(type, "AggregateMetricDouble")) {
                // TODO: support grouping aggregations for aggregate metric double
                return Stream.of(new AggDef(clazz, type, extra, false));
            } else {
                return Stream.of(new AggDef(clazz, type, extra, true), new AggDef(clazz, type, extra, false));
            }
        });
    }

    private static Stream<Tuple<String, String>> combinations(List<String> types, List<String> extraConfigs) {
        return types.stream().flatMap(type -> extraConfigs.stream().map(config -> new Tuple<>(type, config)));
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
            return lookupRetry(clazz, type, extra, grouping);
        } catch (IllegalAccessException | NoSuchMethodException | ClassNotFoundException e) {
            throw new EsqlIllegalArgumentException(e);
        }
    }

    private static MethodHandle lookupRetry(Class<?> clazz, String type, String extra, boolean grouping) throws IllegalAccessException,
        NoSuchMethodException, ClassNotFoundException {
        try {
            return MethodHandles.lookup()
                .findStatic(
                    Class.forName(determineAggName(clazz, type, extra, grouping)),
                    "intermediateStateDesc",
                    MethodType.methodType(List.class)
                );
        } catch (NoSuchMethodException ignore) {
            // Retry without the extra information.
            return MethodHandles.lookup()
                .findStatic(
                    Class.forName(determineAggName(clazz, type, "", grouping)),
                    "intermediateStateDesc",
                    MethodType.methodType(List.class)
                );
        }
    }

    /** Determines the engines agg class name, for the given class, type, and grouping. */
    private static String determineAggName(Class<?> clazz, String type, String extra, boolean grouping) {
        return "org.elasticsearch.compute.aggregation."
            + (clazz.getSimpleName().startsWith("Spatial") ? "spatial." : "")
            + clazz.getSimpleName()
            + type
            + extra
            + (grouping ? "Grouping" : "")
            + "AggregatorFunction";
    }

    /** Maps intermediate state description to named expressions.  */
    private static Stream<NamedExpression> isToNE(List<IntermediateStateDesc> intermediateStateDescs, String aggAlias) {
        return intermediateStateDescs.stream().map(is -> {
            final DataType dataType;
            if (Strings.isEmpty(is.dataType())) {
                dataType = toDataType(is.type());
            } else {
                dataType = DataType.fromEs(is.dataType());
            }
            return new ReferenceAttribute(Source.EMPTY, Attribute.rawTemporaryName(aggAlias, is.name()), dataType);
        });
    }

    /** Returns the data type for the engines element type. */
    // defaults to aggstate, but we'll eventually be able to remove this
    private static DataType toDataType(ElementType elementType) {
        return switch (elementType) {
            case BOOLEAN -> DataType.BOOLEAN;
            case BYTES_REF -> DataType.KEYWORD;
            case INT -> DataType.INTEGER;
            case LONG -> DataType.LONG;
            case DOUBLE -> DataType.DOUBLE;
            case FLOAT, NULL, DOC, COMPOSITE, UNKNOWN -> throw new EsqlIllegalArgumentException("unsupported agg type: " + elementType);
        };
    }

    /** Returns the string representation for the data type. This reflects the engine's aggs naming structure. */
    private static String dataTypeToString(DataType type, Class<?> aggClass) {
        if (aggClass == Count.class) {
            return "";  // no type distinction
        }
        if (aggClass == ToPartial.class || aggClass == FromPartial.class) {
            return "";
        }
        if ((aggClass == Max.class || aggClass == Min.class || aggClass == Top.class) && type.equals(DataType.IP)) {
            return "Ip";
        }

        return switch (type) {
            case BOOLEAN -> "Boolean";
            case INTEGER, COUNTER_INTEGER -> "Int";
            case LONG, DATETIME, COUNTER_LONG, DATE_NANOS -> "Long";
            case DOUBLE, COUNTER_DOUBLE -> "Double";
            case KEYWORD, IP, VERSION, TEXT, SEMANTIC_TEXT -> "BytesRef";
            case GEO_POINT -> "GeoPoint";
            case CARTESIAN_POINT -> "CartesianPoint";
            case GEO_SHAPE -> "GeoShape";
            case CARTESIAN_SHAPE -> "CartesianShape";
            case AGGREGATE_METRIC_DOUBLE -> "AggregateMetricDouble";
            case UNSUPPORTED, NULL, UNSIGNED_LONG, SHORT, BYTE, FLOAT, HALF_FLOAT, SCALED_FLOAT, OBJECT, SOURCE, DATE_PERIOD, TIME_DURATION,
                DOC_DATA_TYPE, TSID_DATA_TYPE, PARTIAL_AGG -> throw new EsqlIllegalArgumentException(
                    "illegal agg type: " + type.typeName()
                );
        };
    }
}
