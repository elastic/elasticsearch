/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.expression.function.grouping;

import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.xpack.esql.core.expression.Alias;
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.expression.FieldAttribute;
import org.elasticsearch.xpack.esql.core.expression.MetadataAttribute;
import org.elasticsearch.xpack.esql.core.expression.NameId;
import org.elasticsearch.xpack.esql.core.expression.Nullability;
import org.elasticsearch.xpack.esql.core.tree.NodeInfo;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.io.stream.PlanStreamInput;

import java.io.IOException;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;

/**
 * Grouping function that keeps time-series grouping generic while excluding the specified dimensions.
 * An empty field list means "group by all dimensions".
 */
public class TimeSeriesWithout extends GroupingFunction.NonEvaluatableGroupingFunction {
    public static final NamedWriteableRegistry.Entry ENTRY = new NamedWriteableRegistry.Entry(
        Expression.class,
        "TimeSeriesWithout",
        TimeSeriesWithout::new
    );

    public TimeSeriesWithout(Source source, List<Expression> fields) {
        super(source, fields);
    }

    public TimeSeriesWithout(StreamInput in) throws IOException {
        this(Source.readFrom((PlanStreamInput) in), in.readNamedWriteableCollectionAsList(Expression.class));
    }

    /**
     * Returns the canonical grouping expression for the {@code _timeseries} metadata column.
     */
    public Alias toAttribute() {
        return toAttribute(null);
    }

    /**
     * Returns the canonical grouping expression for the {@code _timeseries} metadata column.
     */
    public Alias toAttribute(@Nullable NameId id) {
        return new Alias(source(), MetadataAttribute.TIMESERIES, this, id);
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        source().writeTo(out);
        out.writeNamedWriteableCollection(children());
    }

    @Override
    public String getWriteableName() {
        return ENTRY.name;
    }

    @Override
    protected NodeInfo<? extends Expression> info() {
        return NodeInfo.create(this, TimeSeriesWithout::new, children());
    }

    @Override
    public TimeSeriesWithout replaceChildren(List<Expression> newChildren) {
        return new TimeSeriesWithout(source(), newChildren);
    }

    @Override
    public DataType dataType() {
        return DataType.KEYWORD;
    }

    @Override
    public Nullability nullable() {
        return Nullability.FALSE;
    }

    @Override
    protected TypeResolution resolveType() {
        for (Expression field : children()) {
            if (field instanceof FieldAttribute fa) {
                if (fa.isDimension() == false) {
                    return new TypeResolution("WITHOUT requires dimension fields, but [" + fa.sourceText() + "] is not a dimension");
                }
            } else {
                return new TypeResolution(
                    "WITHOUT requires dimension field names, got [" + field.sourceText() + "] of type [" + field.dataType().typeName() + "]"
                );
            }
        }
        return TypeResolution.TYPE_RESOLVED;
    }

    /**
     * Returns the set of field names to exclude from the time-series grouping.
     */
    public Set<String> excludedFieldNames() {
        Set<String> excluded = new LinkedHashSet<>();
        for (Expression field : children()) {
            if (field instanceof FieldAttribute fa) {
                excluded.add(fa.fieldName().string());
            }
        }
        return excluded;
    }

    @Override
    public String toString() {
        return "TimeSeriesWithout{fields=" + children() + "}";
    }
}
