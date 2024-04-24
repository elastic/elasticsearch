/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.expression.function.scalar.convert;

import org.apache.lucene.util.BytesRef;
import org.elasticsearch.compute.ann.ConvertEvaluator;
import org.elasticsearch.xpack.esql.expression.function.Example;
import org.elasticsearch.xpack.esql.expression.function.FunctionInfo;
import org.elasticsearch.xpack.esql.expression.function.Param;
import org.elasticsearch.xpack.ql.expression.Expression;
import org.elasticsearch.xpack.ql.tree.NodeInfo;
import org.elasticsearch.xpack.ql.tree.Source;
import org.elasticsearch.xpack.ql.type.DataType;

import java.util.List;
import java.util.Map;

import static org.elasticsearch.xpack.esql.type.EsqlDataTypeConverter.stringToIP;
import static org.elasticsearch.xpack.ql.type.DataTypes.IP;
import static org.elasticsearch.xpack.ql.type.DataTypes.KEYWORD;
import static org.elasticsearch.xpack.ql.type.DataTypes.TEXT;

public class ToIP extends AbstractConvertFunction {

    private static final Map<DataType, BuildFactory> EVALUATORS = Map.ofEntries(
        Map.entry(IP, (field, source) -> field),
        Map.entry(KEYWORD, ToIPFromStringEvaluator.Factory::new),
        Map.entry(TEXT, ToIPFromStringEvaluator.Factory::new)
    );

    @FunctionInfo(
        returnType = "ip",
        description = "Converts an input string to an IP value.",
        examples = @Example(file = "ip", tag = "to_ip", explanation = """
            Note that in this example, the last conversion of the string isn't possible.
            When this happens, the result is a *null* value. In this case a _Warning_ header is added to the response.
            The header will provide information on the source of the failure:

            `"Line 1:68: evaluation of [TO_IP(str2)] failed, treating result as null. Only first 20 failures recorded."`

            A following header will contain the failure reason and the offending value:

            `"java.lang.IllegalArgumentException: 'foo' is not an IP string literal."`""")
    )
    public ToIP(
        Source source,
        @Param(
            name = "field",
            type = { "ip", "keyword", "text" },
            description = "Input value. The input can be a single- or multi-valued column or an expression."
        ) Expression field
    ) {
        super(source, field);
    }

    @Override
    protected Map<DataType, BuildFactory> factories() {
        return EVALUATORS;
    }

    @Override
    public DataType dataType() {
        return IP;
    }

    @Override
    public Expression replaceChildren(List<Expression> newChildren) {
        return new ToIP(source(), newChildren.get(0));
    }

    @Override
    protected NodeInfo<? extends Expression> info() {
        return NodeInfo.create(this, ToIP::new, field());
    }

    @ConvertEvaluator(extraName = "FromString", warnExceptions = { IllegalArgumentException.class })
    static BytesRef fromKeyword(BytesRef asString) {
        return stringToIP(asString);
    }
}
