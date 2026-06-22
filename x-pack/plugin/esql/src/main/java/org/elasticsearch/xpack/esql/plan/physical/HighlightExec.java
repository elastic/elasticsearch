/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.plan.physical;

import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.expression.MapExpression;
import org.elasticsearch.xpack.esql.core.tree.NodeInfo;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.io.stream.PlanStreamInput;

import java.io.IOException;
import java.util.List;
import java.util.Objects;

public class HighlightExec extends UnaryExec {

    public static final NamedWriteableRegistry.Entry ENTRY = new NamedWriteableRegistry.Entry(
        PhysicalPlan.class,
        "HighlightExec",
        HighlightExec::new
    );

    private final String prefix;
    private final Expression query;
    private final List<Expression> fields;
    private final MapExpression options;

    public HighlightExec(
        Source source,
        PhysicalPlan child,
        String prefix,
        Expression query,
        List<Expression> fields,
        MapExpression options
    ) {
        super(source, child);
        this.prefix = prefix;
        this.query = query;
        this.fields = fields;
        this.options = options;
    }

    private HighlightExec(StreamInput in) throws IOException {
        this(
            Source.readFrom((PlanStreamInput) in),
            in.readNamedWriteable(PhysicalPlan.class),
            in.readString(),
            in.readOptionalNamedWriteable(Expression.class),
            in.readNamedWriteableCollectionAsList(Expression.class),
            in.readOptionalNamedWriteable(MapExpression.class)
        );
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        source().writeTo(out);
        out.writeNamedWriteable(child());
        out.writeString(prefix);
        out.writeOptionalNamedWriteable(query);
        out.writeNamedWriteableCollection(fields);
        out.writeOptionalNamedWriteable(options);
    }

    @Override
    public String getWriteableName() {
        return ENTRY.name;
    }

    public String prefix() {
        return prefix;
    }

    public Expression query() {
        return query;
    }

    public List<Expression> fields() {
        return fields;
    }

    public MapExpression options() {
        return options;
    }

    @Override
    public HighlightExec replaceChild(PhysicalPlan newChild) {
        return new HighlightExec(source(), newChild, prefix, query, fields, options);
    }

    @Override
    protected NodeInfo<HighlightExec> info() {
        return NodeInfo.create(this, HighlightExec::new, child(), prefix, query, fields, options);
    }

    @Override
    public int hashCode() {
        return Objects.hash(super.hashCode(), prefix, query, fields, options);
    }

    @Override
    public boolean equals(Object obj) {
        if (super.equals(obj) == false) {
            return false;
        }
        HighlightExec other = (HighlightExec) obj;
        return Objects.equals(prefix, other.prefix)
            && Objects.equals(query, other.query)
            && Objects.equals(fields, other.fields)
            && Objects.equals(options, other.options);
    }
}
