/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.expression.function.aggregate;

import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.compute.aggregation.StdDevStates;
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.expression.Literal;
import org.elasticsearch.xpack.esql.core.tree.NodeInfo;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.expression.SurrogateExpression;
import org.elasticsearch.xpack.esql.expression.function.Example;
import org.elasticsearch.xpack.esql.expression.function.FunctionInfo;
import org.elasticsearch.xpack.esql.expression.function.FunctionType;
import org.elasticsearch.xpack.esql.expression.function.Param;

import java.io.IOException;
import java.util.Collections;
import java.util.List;

import static java.util.Collections.emptyList;
import static org.elasticsearch.xpack.esql.core.expression.TypeResolutions.ParamOrdinal.DEFAULT;
import static org.elasticsearch.xpack.esql.core.expression.TypeResolutions.isType;

public class $Variation$ extends AggregateFunction implements SurrogateExpression {
    public static final NamedWriteableRegistry.Entry ENTRY = new NamedWriteableRegistry.Entry(
        Expression.class,
        "$Variation$",
        $Variation$::new
    );

    @FunctionInfo(
        returnType = "double",
        description = "The $lower_case$ of a numeric field.",
        type = FunctionType.AGGREGATE,
        examples = {
            @Example(file = "stats", tag = "$Variation$"),
            @Example(
                description = "The expression can use inline functions. For example, to calculate the "
                    + "$lower_case$ of each employee’s maximum salary changes, "
                    + "first use `MV_MAX` on each row, and then use `$CAPS$` on the result",
                file = "stats",
                tag = "docsStats$Variation$NestedExpression"
            ) }
    )
    public $Variation$(Source source, @Param(name = "number", type = { "double", "integer", "long" }) Expression field) {
        this(source, field, Literal.TRUE);
    }

    public $Variation$(Source source, Expression field, Expression filter) {
        super(source, field, filter, emptyList());
    }

    private $Variation$(StreamInput in) throws IOException {
        super(in);
    }

    @Override
    public String getWriteableName() {
        return ENTRY.name;
    }

    @Override
    public DataType dataType() {
        return DataType.DOUBLE;
    }

    @Override
    protected Expression.TypeResolution resolveType() {
        return isType(
            field(),
            dt -> dt.isNumeric() && dt != DataType.UNSIGNED_LONG,
            sourceText(),
            DEFAULT,
            "numeric except unsigned_long or counter types"
        );
    }

    @Override
    protected NodeInfo<$Variation$> info() {
        return NodeInfo.create(this, $Variation$::new, field(), filter());
    }

    @Override
    public $Variation$ replaceChildren(List<Expression> newChildren) {
        return new $Variation$(source(), newChildren.get(0), newChildren.get(1));
    }

    public $Variation$ withFilter(Expression filter) {
        return new $Variation$(source(), field(), filter);
    }

    @Override
    public Expression surrogate() {
        $if(StdDevSample)$
        return new StdDev(source(), field(), filter(), new Literal(source(), StdDevStates.Variation.$enum$.getIndex(), DataType.INTEGER));

        $else$
        return new StdDev(
            source(),
            field(),
            filter(),
            new Literal(source(), StdDevStates.Variation.$enum$.getIndex(), DataType.INTEGER)
        );
        $endif$
    }
}
