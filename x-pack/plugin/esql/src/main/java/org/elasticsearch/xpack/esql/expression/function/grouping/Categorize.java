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
import org.elasticsearch.license.XPackLicenseState;
import org.elasticsearch.xpack.esql.LicenseAware;
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.expression.Nullability;
import org.elasticsearch.xpack.esql.core.tree.NodeInfo;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.expression.function.Example;
import org.elasticsearch.xpack.esql.expression.function.FunctionInfo;
import org.elasticsearch.xpack.esql.expression.function.FunctionType;
import org.elasticsearch.xpack.esql.expression.function.Param;
import org.elasticsearch.xpack.esql.io.stream.PlanStreamInput;
import org.elasticsearch.xpack.ml.MachineLearning;

import java.io.IOException;
import java.util.List;

import static org.elasticsearch.xpack.esql.core.expression.TypeResolutions.ParamOrdinal.DEFAULT;
import static org.elasticsearch.xpack.esql.core.expression.TypeResolutions.isString;

/**
 * Categorizes text messages.
 * <p>
 *     This function has no evaluators, as it works like an aggregation (Accumulates values, stores intermediate states, etc).
 * </p>
 * <p>
 *     For the implementation, see {@link org.elasticsearch.compute.aggregation.blockhash.CategorizeBlockHash}
 * </p>
 */
public class Categorize extends GroupingFunction.NonEvaluatableGroupingFunction implements LicenseAware {
    public static final NamedWriteableRegistry.Entry ENTRY = new NamedWriteableRegistry.Entry(
        Expression.class,
        "Categorize",
        Categorize::new
    );

    private final Expression field;

    @FunctionInfo(
        returnType = "keyword",
        description = "Groups text messages into categories of similarly formatted text values.",
        detailedDescription = """
            `CATEGORIZE` has the following limitations:

            * can’t be used within other expressions
            * can’t be used more than once in the groupings
            * can’t be used or referenced within aggregate functions and it has to be the first grouping""",
        examples = {
            @Example(
                file = "docs",
                tag = "docsCategorize",
                description = "This example categorizes server logs messages into categories and aggregates their counts. "
            ) },
        preview = true,
        type = FunctionType.GROUPING
    )
    public Categorize(
        Source source,
        @Param(name = "field", type = { "text", "keyword" }, description = "Expression to categorize") Expression field

    ) {
        super(source, List.of(field));
        this.field = field;
    }

    private Categorize(StreamInput in) throws IOException {
        this(Source.readFrom((PlanStreamInput) in), in.readNamedWriteable(Expression.class));
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        source().writeTo(out);
        out.writeNamedWriteable(field);
    }

    @Override
    public String getWriteableName() {
        return ENTRY.name;
    }

    @Override
    public boolean foldable() {
        // Categorize cannot be currently folded
        return false;
    }

    @Override
    public Nullability nullable() {
        // Null strings and strings that don’t produce tokens after analysis lead to null values.
        // This includes empty strings, only whitespace, (hexa)decimal numbers and stopwords.
        return Nullability.TRUE;
    }

    @Override
    protected TypeResolution resolveType() {
        return isString(field(), sourceText(), DEFAULT);
    }

    @Override
    public DataType dataType() {
        return DataType.KEYWORD;
    }

    @Override
    public Categorize replaceChildren(List<Expression> newChildren) {
        return new Categorize(source(), newChildren.get(0));
    }

    @Override
    protected NodeInfo<? extends Expression> info() {
        return NodeInfo.create(this, Categorize::new, field);
    }

    public Expression field() {
        return field;
    }

    @Override
    public String toString() {
        return "Categorize{field=" + field + "}";
    }

    @Override
    public boolean licenseCheck(XPackLicenseState state) {
        return MachineLearning.CATEGORIZE_TEXT_AGG_FEATURE.check(state);
    }
}
