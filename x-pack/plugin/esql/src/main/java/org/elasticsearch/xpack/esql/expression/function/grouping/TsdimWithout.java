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
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.expression.Expressions;
import org.elasticsearch.xpack.esql.core.expression.FieldAttribute;
import org.elasticsearch.xpack.esql.core.tree.NodeInfo;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.expression.function.FunctionInfo;
import org.elasticsearch.xpack.esql.expression.function.FunctionType;
import org.elasticsearch.xpack.esql.expression.function.OptionalArgument;
import org.elasticsearch.xpack.esql.expression.function.Param;
import org.elasticsearch.xpack.esql.io.stream.PlanStreamInput;

import java.io.IOException;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Stream;

import static org.elasticsearch.xpack.esql.core.expression.TypeResolutions.ParamOrdinal.DEFAULT;
import static org.elasticsearch.xpack.esql.core.expression.TypeResolutions.isType;

/**
 * Excludes one or more dimension fields from the _timeseries result.
 * This function can only be used in time-series aggregations.
 */
public class TsdimWithout extends GroupingFunction.NonEvaluatableGroupingFunction implements OptionalArgument {
    public static final NamedWriteableRegistry.Entry ENTRY = new NamedWriteableRegistry.Entry(
        Expression.class,
        "TsdimWithout",
        TsdimWithout::new
    );

    public static final String NAME = "TSDIM_WITHOUT";

    @FunctionInfo(
        returnType = "keyword",
        description = "Excludes one or more dimension fields from the _timeseries result in time-series aggregations.",
        examples = {
        // @Example(description = "Exclude a single field from _timeseries", file = "tsdim_without", tag = "docsTsdimWithoutSingle"),
        // @Example(description = "Exclude multiple fields from _timeseries", file = "tsdim_without", tag = "docsTsdimWithoutMultiple")
        },
        type = FunctionType.GROUPING
    )
    public TsdimWithout(
        Source source,
        @Param(name = "field1", type = { "keyword", "text" }, description = "Dimension field to exclude") Expression first,
        @Param(name = "field2", type = { "keyword", "text" }, description = "Dimension field to exclude") List<Expression> rest
    ) {
        super(source, Stream.concat(Stream.of(first), rest.stream()).toList());
    }

    private TsdimWithout(StreamInput in) throws IOException {
        this(
            Source.readFrom((PlanStreamInput) in),
            in.readNamedWriteable(Expression.class),
            in.readNamedWriteableCollectionAsList(Expression.class)
        );
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        source().writeTo(out);
        out.writeNamedWriteable(children().get(0));
        out.writeNamedWriteableCollection(children().subList(1, children().size()));
    }

    @Override
    public String getWriteableName() {
        return ENTRY.name;
    }

    @Override
    protected TypeResolution resolveType() {
        if (childrenResolved() == false) {
            return new TypeResolution("Unresolved children");
        }

        if (children().isEmpty()) {
            return new TypeResolution("expects at least one argument");
        }

        TypeResolution resolution = TypeResolution.TYPE_RESOLVED;
        for (Expression field : children()) {
            resolution = resolution.and(
                isType(field, dt -> dt == DataType.KEYWORD || dt == DataType.TEXT, sourceText(), DEFAULT, "keyword or text")
            );
        }

        return resolution;
    }

    @Override
    public boolean foldable() {
        return Expressions.foldable(children());
    }

    @Override
    public DataType dataType() {
        return DataType.KEYWORD;
    }

    @Override
    public Expression replaceChildren(List<Expression> newChildren) {
        return new TsdimWithout(source(), newChildren.get(0), newChildren.subList(1, newChildren.size()));
    }

    @Override
    protected NodeInfo<? extends Expression> info() {
        return NodeInfo.create(this, TsdimWithout::new, children().get(0), children().subList(1, children().size()));
    }

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
        return "TsdimWithout{fields=" + children() + "}";
    }
}
