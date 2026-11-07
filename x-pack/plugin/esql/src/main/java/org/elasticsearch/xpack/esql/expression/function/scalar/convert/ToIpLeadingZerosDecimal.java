/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.expression.function.scalar.convert;

import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.tree.NodeInfo;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.type.DataType;

import java.io.IOException;
import java.util.List;
import java.util.Map;

import static org.elasticsearch.xpack.esql.core.type.DataType.INTEGER;
import static org.elasticsearch.xpack.esql.core.type.DataType.IP;
import static org.elasticsearch.xpack.esql.core.type.DataType.KEYWORD;
import static org.elasticsearch.xpack.esql.core.type.DataType.LONG;
import static org.elasticsearch.xpack.esql.core.type.DataType.TEXT;
import static org.elasticsearch.xpack.esql.core.type.DataType.UNSIGNED_LONG;
import static org.elasticsearch.xpack.esql.expression.function.scalar.convert.ParseIp.FROM_INTEGER;
import static org.elasticsearch.xpack.esql.expression.function.scalar.convert.ParseIp.FROM_KEYWORD_LEADING_ZEROS_DECIMAL;
import static org.elasticsearch.xpack.esql.expression.function.scalar.convert.ParseIp.FROM_LONG;
import static org.elasticsearch.xpack.esql.expression.function.scalar.convert.ParseIp.FROM_UNSIGNED_LONG;

public class ToIpLeadingZerosDecimal extends AbstractConvertFunction {
    public static final NamedWriteableRegistry.Entry ENTRY = new NamedWriteableRegistry.Entry(
        Expression.class,
        "ToIpLeadingZerosDecimal",
        ToIpLeadingZerosDecimal::new
    );

    static final Map<DataType, BuildFactory> EVALUATORS = Map.ofEntries(
        Map.entry(IP, (source, field) -> field),
        Map.entry(KEYWORD, FROM_KEYWORD_LEADING_ZEROS_DECIMAL),
        Map.entry(TEXT, FROM_KEYWORD_LEADING_ZEROS_DECIMAL),
        Map.entry(INTEGER, FROM_INTEGER),
        Map.entry(LONG, FROM_LONG),
        Map.entry(UNSIGNED_LONG, FROM_UNSIGNED_LONG)
    );

    public ToIpLeadingZerosDecimal(Source source, Expression field) {
        super(source, field);
    }

    private ToIpLeadingZerosDecimal(StreamInput in) throws IOException {
        super(in);
    }

    @Override
    public String getWriteableName() {
        return ENTRY.name;
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
        return new ToIpLeadingZerosDecimal(source(), newChildren.get(0));
    }

    @Override
    protected NodeInfo<? extends Expression> info() {
        return NodeInfo.create(this, ToIpLeadingZerosDecimal::new, field());
    }
}
