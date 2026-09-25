/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.expression.function.scalar.multivalue;

import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.tree.NodeInfo;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.expression.function.Example;
import org.elasticsearch.xpack.esql.expression.function.FunctionAppliesTo;
import org.elasticsearch.xpack.esql.expression.function.FunctionAppliesToLifecycle;
import org.elasticsearch.xpack.esql.expression.function.FunctionDefinition;
import org.elasticsearch.xpack.esql.expression.function.FunctionInfo;
import org.elasticsearch.xpack.esql.expression.function.MapParam;
import org.elasticsearch.xpack.esql.expression.function.Param;

import java.io.IOException;
import java.util.List;

/** {@code true} if any value of {@code field} is greater than {@code bound}. See {@link MvCompare}. */
public class MvGreater extends MvCompare {
    public static final NamedWriteableRegistry.Entry ENTRY = new NamedWriteableRegistry.Entry(
        Expression.class,
        "MvGreater",
        MvGreater::new
    );

    public static final FunctionDefinition DEFINITION = FunctionDefinition.def(MvGreater.class).ternary(MvGreater::new).name("mv_greater");

    @FunctionInfo(
        returnType = "boolean",
        briefSummary = "Checks whether a multivalue field has any value greater than a bound.",
        description = "Returns `true` if at least one value of `field` is greater than `bound`, "
            + "using the natural order of the type. A null or empty field returns `false`, as does a null or "
            + "multivalued bound. The comparison is strict (`>`) by default; set `include_bound` to `true` in the "
            + "optional `options` map to make it inclusive (`>=`). Works on any ordered type: numbers, dates, IPs, "
            + "versions, and strings (compared by their UTF-8 bytes).",
        examples = {
            @Example(file = "mv_compare", tag = "mv_greater"),
            @Example(
                description = "With `include_bound: true` a value equal to the bound matches too:",
                file = "mv_compare",
                tag = "mv_greater_include_bound"
            ),
            @Example(description = "Strings are compared by their UTF-8 bytes:", file = "mv_compare", tag = "mv_greater_keyword") },
        preview = true,
        appliesTo = { @FunctionAppliesTo(lifeCycle = FunctionAppliesToLifecycle.PREVIEW, version = "9.6.0") }
    )
    public MvGreater(
        Source source,
        @Param(
            name = "field",
            type = { "date", "date_nanos", "double", "integer", "ip", "keyword", "long", "text", "unsigned_long", "version" },
            description = "Multivalue expression to test. If null or empty, the function returns `false`."
        ) Expression field,
        @Param(
            name = "bound",
            type = { "date", "date_nanos", "double", "integer", "ip", "keyword", "long", "text", "unsigned_long", "version" },
            description = "Comparison bound, of the same type as `field`. If null or multivalued, the function returns `false`."
        ) Expression bound,
        @MapParam(
            name = "options",
            params = {
                @MapParam.MapParamEntry(
                    name = "include_bound",
                    type = "boolean",
                    valueHint = { "true", "false" },
                    description = "Whether the bound is inclusive. Defaults to `false` (strict `>`); `true` makes it inclusive (`>=`)."
                ) },
            description = "(Optional) Bound inclusivity options.",
            optional = true
        ) Expression options
    ) {
        super(source, field, bound, options, true);
    }

    public MvGreater(Source source, Expression field, Expression bound) {
        this(source, field, bound, null);
    }

    private MvGreater(StreamInput in) throws IOException {
        super(in, true);
    }

    @Override
    public String getWriteableName() {
        return ENTRY.name;
    }

    @Override
    public Expression replaceChildren(List<Expression> newChildren) {
        return new MvGreater(source(), newChildren.get(0), newChildren.get(1), newChildren.size() > 2 ? newChildren.get(2) : null);
    }

    @Override
    protected NodeInfo<? extends Expression> info() {
        return NodeInfo.create(this, MvGreater::new, field(), bound(), options());
    }
}
