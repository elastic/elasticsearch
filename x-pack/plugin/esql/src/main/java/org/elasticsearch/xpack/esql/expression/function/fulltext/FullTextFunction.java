/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.expression.function.fulltext;

import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.xpack.esql.action.EsqlCapabilities;
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.expression.function.Function;
import org.elasticsearch.xpack.esql.core.querydsl.query.Query;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.core.util.PlanStreamInput;
import org.elasticsearch.xpack.esql.evaluator.mapper.EvaluatorMapper;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import static java.util.Collections.singletonList;

public abstract class FullTextFunction extends Function implements EvaluatorMapper {
    public static List<NamedWriteableRegistry.Entry> getNamedWriteables() {
        List<NamedWriteableRegistry.Entry> entries = new ArrayList<>();
        if (EsqlCapabilities.Cap.QSTR_FUNCTION.isEnabled()) {
            entries.add(QueryStringFunction.ENTRY);
        }
        return entries;
    }

    private final Expression query;

    protected FullTextFunction(Source source, Expression query) {
        super(source, singletonList(query));
        this.query = query;
    }

    protected FullTextFunction(StreamInput in) throws IOException {
        this(Source.readFrom((StreamInput & PlanStreamInput) in), in.readNamedWriteable(Expression.class));
    }

    @Override
    public DataType dataType() {
        return DataType.BOOLEAN;
    }

    public Expression query() {
        return query;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        source().writeTo(out);
        out.writeNamedWriteable(query);
    }

    public abstract Query asQuery();

    protected static String unquoteQueryString(String quotedString) {
        if (quotedString.length() < 2) {
            return quotedString;
        }
        return quotedString.substring(1, quotedString.length() - 1);
    }
}
