/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.expression.function.scalar.string;

import org.elasticsearch.TransportVersions;
import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.tree.NodeInfo;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.expression.function.Example;
import org.elasticsearch.xpack.esql.expression.function.FunctionInfo;
import org.elasticsearch.xpack.esql.expression.function.Param;
import org.elasticsearch.xpack.esql.io.stream.PlanStreamInput;
import org.elasticsearch.xpack.esql.session.Configuration;

import java.io.IOException;

public class ToLower extends ChangeCase {
    public static final NamedWriteableRegistry.Entry ENTRY = new NamedWriteableRegistry.Entry(Expression.class, "ToLower", ToLower::new);

    @FunctionInfo(
        returnType = { "keyword" },
        description = "Returns a new string representing the input string converted to lower case.",
        examples = { @Example(file = "string", tag = "to_lower"), @Example(file = "string", tag = "to_lower_mv") }
    )

    public ToLower(Source source, @Param(name = "str", type = { "keyword", "text" }, description = """
        String expression. If `null`, the function returns `null`. The input can be a single-valued column or expression.
        """)
    Expression field, Configuration configuration) {
        super(source, field, configuration, Case.LOWER);
    }

    private ToLower(StreamInput in) throws IOException {
        this(
            in.getTransportVersion().onOrAfter(TransportVersions.ESQL_SERIALIZE_SOURCE_FUNCTIONS_WARNINGS)
                ? Source.readFrom((PlanStreamInput) in)
                : Source.EMPTY,
            in.readNamedWriteable(Expression.class),
            ((PlanStreamInput) in).configuration()
        );
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        if (out.getTransportVersion().onOrAfter(TransportVersions.ESQL_SERIALIZE_SOURCE_FUNCTIONS_WARNINGS)) {
            source().writeTo(out);
        }
        out.writeNamedWriteable(field());
    }

    @Override
    public String getWriteableName() {
        return ENTRY.name;
    }

    public ToLower replaceChild(Expression child) {
        return new ToLower(source(), child, configuration());
    }

    @Override
    protected NodeInfo<? extends Expression> info() {
        return NodeInfo.create(this, ToLower::new, field(), configuration());
    }
}
