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
import org.elasticsearch.xpack.esql.core.expression.FieldAttribute;
import org.elasticsearch.xpack.esql.core.expression.Nullability;
import org.elasticsearch.xpack.esql.core.tree.NodeInfo;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.io.stream.PlanStreamInput;

import java.io.IOException;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

/**
 * Grouping function that modifies the effective {@code _tsid} grouping to exclude the specified dimension fields.
 */
public class TsdimWithout extends GroupingFunction.NonEvaluatableGroupingFunction {
    public static final NamedWriteableRegistry.Entry ENTRY = new NamedWriteableRegistry.Entry(
        Expression.class,
        "TsdimWithout",
        TsdimWithout::new
    );

    public TsdimWithout(Source source, List<Expression> fields) {
        super(source, fields);
    }

    public TsdimWithout(StreamInput in) throws IOException {
        this(Source.readFrom((PlanStreamInput) in), in.readNamedWriteableCollectionAsList(Expression.class));
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
        return NodeInfo.create(this, TsdimWithout::new, children());
    }

    @Override
    public TsdimWithout replaceChildren(List<Expression> newChildren) {
        return new TsdimWithout(source(), newChildren);
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
            if (field instanceof FieldAttribute == false) {
                return new TypeResolution("TSDIM_WITHOUT requires field attributes, got [" + field.sourceText() + "]");
            }
        }
        return TypeResolution.TYPE_RESOLVED;
    }

    /**
     * Returns the set of field names to exclude from the time-series grouping.
     */
    public Set<String> excludedFieldNames() {
        Set<String> excluded = new HashSet<>();
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
