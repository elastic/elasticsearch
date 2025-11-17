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
import org.elasticsearch.xpack.esql.EsqlIllegalArgumentException;
import org.elasticsearch.xpack.esql.core.expression.Alias;
import org.elasticsearch.xpack.esql.core.expression.Attribute;
import org.elasticsearch.xpack.esql.core.expression.AttributeMap;
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.expression.FieldAttribute;
import org.elasticsearch.xpack.esql.core.expression.MetadataAttribute;
import org.elasticsearch.xpack.esql.core.expression.NamedExpression;
import org.elasticsearch.xpack.esql.core.expression.ReferenceAttribute;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.expression.function.aggregate.AggregateFunction;

import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Stream;

/**
 * Static class used to convert aggregate expressions to the named expressions that represent their intermediate state.
 */
public final class AggregateMapper {

    public static List<NamedExpression> mapNonGrouping(List<? extends NamedExpression> aggregates) {
        return doMapping(aggregates, false);
    }

    public static List<NamedExpression> mapGrouping(List<? extends NamedExpression> aggregates) {
        return doMapping(aggregates, true);
    }

    private static List<NamedExpression> doMapping(List<? extends NamedExpression> aggregates, boolean grouping) {
        Set<Expression> seen = new HashSet<>();
        AttributeMap.Builder<NamedExpression> attrToExpressionsBuilder = AttributeMap.builder();
        for (NamedExpression agg : aggregates) {
            Expression inner = Alias.unwrap(agg);
            if (seen.add(inner)) {
                for (var ne : computeEntryForAgg(agg.name(), inner, grouping)) {
                    attrToExpressionsBuilder.put(ne.toAttribute(), ne);
                }
            }
        }
        return attrToExpressionsBuilder.build().values().stream().toList();
    }

    public static List<IntermediateStateDesc> intermediateStateDesc(AggregateFunction fn, boolean grouping) {
        if (fn instanceof ToAggregator toAggregator) {
            var supplier = toAggregator.supplier();
            return grouping ? supplier.groupingIntermediateStateDesc() : supplier.nonGroupingIntermediateStateDesc();
        } else {
            throw new EsqlIllegalArgumentException("Aggregate has no defined intermediate state: " + fn);
        }
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
        List<IntermediateStateDesc> intermediateState;
        if (aggregateFunction instanceof ToAggregator toAggregator) {
            var supplier = toAggregator.supplier();
            intermediateState = grouping ? supplier.groupingIntermediateStateDesc() : supplier.nonGroupingIntermediateStateDesc();
        } else {
            throw new EsqlIllegalArgumentException("Aggregate has no defined intermediate state: " + aggregateFunction);
        }
        return intermediateStateToNamedExpressions(intermediateState, aggAlias).toList();
    }

    /** Maps intermediate state description to named expressions.  */
    private static Stream<NamedExpression> intermediateStateToNamedExpressions(
        List<IntermediateStateDesc> intermediateStateDescs,
        String aggAlias
    ) {
        return intermediateStateDescs.stream().map(is -> {
            final DataType dataType;
            if (Strings.isEmpty(is.dataType())) {
                dataType = toDataType(is.type());
            } else {
                dataType = DataType.fromEs(is.dataType());
            }
            return new ReferenceAttribute(Source.EMPTY, null, Attribute.rawTemporaryName(aggAlias, is.name()), dataType);
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
            case DOC -> DataType.DOC_DATA_TYPE;
            case EXPONENTIAL_HISTOGRAM -> DataType.EXPONENTIAL_HISTOGRAM;
            case FLOAT, NULL, COMPOSITE, AGGREGATE_METRIC_DOUBLE, UNKNOWN -> throw new EsqlIllegalArgumentException(
                "unsupported agg type: " + elementType
            );
        };
    }
}
