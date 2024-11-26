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
import org.elasticsearch.xpack.esql.capabilities.Validatable;
import org.elasticsearch.xpack.esql.common.Failure;
import org.elasticsearch.xpack.esql.common.Failures;
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.expression.FieldAttribute;
import org.elasticsearch.xpack.esql.core.expression.TypeResolutions;
import org.elasticsearch.xpack.esql.core.querydsl.query.QueryStringQuery;
import org.elasticsearch.xpack.esql.core.tree.NodeInfo;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.expression.function.Example;
import org.elasticsearch.xpack.esql.expression.function.FunctionInfo;
import org.elasticsearch.xpack.esql.expression.function.Param;
import org.elasticsearch.xpack.esql.io.stream.PlanStreamInput;

import java.io.IOException;
import java.util.List;
import java.util.Locale;
import java.util.Set;

import static org.elasticsearch.xpack.esql.core.expression.TypeResolutions.ParamOrdinal.FIRST;
import static org.elasticsearch.xpack.esql.core.expression.TypeResolutions.ParamOrdinal.SECOND;
import static org.elasticsearch.xpack.esql.core.expression.TypeResolutions.isType;
import static org.elasticsearch.xpack.esql.core.type.DataType.BOOLEAN;
import static org.elasticsearch.xpack.esql.core.type.DataType.DATETIME;
import static org.elasticsearch.xpack.esql.core.type.DataType.DATE_NANOS;
import static org.elasticsearch.xpack.esql.core.type.DataType.DOUBLE;
import static org.elasticsearch.xpack.esql.core.type.DataType.INTEGER;
import static org.elasticsearch.xpack.esql.core.type.DataType.IP;
import static org.elasticsearch.xpack.esql.core.type.DataType.KEYWORD;
import static org.elasticsearch.xpack.esql.core.type.DataType.LONG;
import static org.elasticsearch.xpack.esql.core.type.DataType.TEXT;
import static org.elasticsearch.xpack.esql.core.type.DataType.UNSIGNED_LONG;
import static org.elasticsearch.xpack.esql.core.type.DataType.VERSION;

/**
 * Full text function that performs a {@link QueryStringQuery} .
 */
public class Match extends FullTextFunction implements Validatable {

    public static final NamedWriteableRegistry.Entry ENTRY = new NamedWriteableRegistry.Entry(Expression.class, "Match", Match::readFrom);

    private final Expression field;

    private transient Boolean isOperator;

    public static final Set<DataType> DATA_TYPES = Set.of(
        KEYWORD,
        TEXT,
        BOOLEAN,
        DATETIME,
        DATE_NANOS,
        DOUBLE,
        INTEGER,
        IP,
        LONG,
        UNSIGNED_LONG,
        VERSION
    );

    @FunctionInfo(
        returnType = "boolean",
        preview = true,
        description = "Performs a match query on the specified field. Returns true if the provided query matches the row.",
        examples = { @Example(file = "match-function", tag = "match-with-field") }
    )
    public Match(
        Source source,
        @Param(
            name = "field",
            type = { "keyword", "text", "boolean", "date", "date_nanos", "double", "integer", "ip", "long", "unsigned_long", "version" },
            description = "Field that the query will target."
        ) Expression field,
        @Param(
            name = "query",
            type = { "keyword", "text", "boolean", "date", "date_nanos", "double", "integer", "ip", "long", "unsigned_long", "version" },
            description = "Text you wish to find in the provided field."
        ) Expression matchQuery
    ) {
        super(source, matchQuery, List.of(field, matchQuery));
        this.field = field;
    }

    private static Match readFrom(StreamInput in) throws IOException {
        Source source = Source.readFrom((PlanStreamInput) in);
        Expression field = in.readNamedWriteable(Expression.class);
        Expression query = in.readNamedWriteable(Expression.class);
        return new Match(source, field, query);
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        source().writeTo(out);
        out.writeNamedWriteable(field());
        out.writeNamedWriteable(query());
    }

    @Override
    public String getWriteableName() {
        return ENTRY.name;
    }

    @Override
    protected TypeResolution resolveNonQueryParamTypes() {
        return isType(
            field,
            DATA_TYPES::contains,
            functionName(),
            FIRST,
            "keyword, text, boolean, date, date_nanos, double, integer, ip, long, unsigned_long, version"
        );
    }

    @Override
    protected TypeResolution resolveQueryParamType() {
        return isType(
            query(),
            DATA_TYPES::contains,
            functionName(),
            queryParamOrdinal(),
            "keyword, text, boolean, date, date_nanos, double, integer, ip, long, unsigned_long, version"
        );
    }

    @Override
    public void validate(Failures failures) {
        if (field instanceof FieldAttribute == false) {
            failures.add(
                Failure.fail(
                    field,
                    "[{}] {} cannot operate on [{}], which is not a field from an index mapping",
                    functionName(),
                    functionType(),
                    field.sourceText()
                )
            );
        }
    }

    @Override
    public Expression replaceChildren(List<Expression> newChildren) {
        return new Match(source(), newChildren.get(0), newChildren.get(1));
    }

    @Override
    protected NodeInfo<? extends Expression> info() {
        return NodeInfo.create(this, Match::new, field, query());
    }

    protected TypeResolutions.ParamOrdinal queryParamOrdinal() {
        return SECOND;
    }

    public Expression field() {
        return field;
    }

    @Override
    public String functionType() {
        return isOperator() ? "operator" : super.functionType();
    }

    @Override
    public String functionName() {
        return isOperator() ? ":" : super.functionName();
    }

    private boolean isOperator() {
        if (isOperator == null) {
            isOperator = source().text().toUpperCase(Locale.ROOT).matches("^" + super.functionName() + "\\s*\\(.*\\)") == false;
        }
        return isOperator;
    }
}
