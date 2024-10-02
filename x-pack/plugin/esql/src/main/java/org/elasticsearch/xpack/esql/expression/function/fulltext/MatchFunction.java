/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.expression.function.fulltext;

import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.xpack.esql.capabilities.Validatable;
import org.elasticsearch.xpack.esql.common.Failure;
import org.elasticsearch.xpack.esql.common.Failures;
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.expression.FieldAttribute;
import org.elasticsearch.xpack.esql.core.expression.TypeResolutions;
import org.elasticsearch.xpack.esql.core.querydsl.query.MatchQuery;
import org.elasticsearch.xpack.esql.core.querydsl.query.Query;
import org.elasticsearch.xpack.esql.core.querydsl.query.QueryStringQuery;
import org.elasticsearch.xpack.esql.core.tree.NodeInfo;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.expression.function.Example;
import org.elasticsearch.xpack.esql.expression.function.FunctionInfo;
import org.elasticsearch.xpack.esql.expression.function.Param;
import org.elasticsearch.xpack.esql.io.stream.PlanStreamInput;

import java.io.IOException;
import java.util.List;

import static org.elasticsearch.xpack.esql.core.expression.TypeResolutions.ParamOrdinal.FIRST;
import static org.elasticsearch.xpack.esql.core.expression.TypeResolutions.ParamOrdinal.SECOND;
import static org.elasticsearch.xpack.esql.core.expression.TypeResolutions.isNotNull;
import static org.elasticsearch.xpack.esql.core.expression.TypeResolutions.isString;

/**
 * Full text function that performs a {@link QueryStringQuery} .
 */
public class MatchFunction extends FullTextFunction implements Validatable {

    public static final NamedWriteableRegistry.Entry ENTRY = new NamedWriteableRegistry.Entry(
        Expression.class,
        "Match",
        MatchFunction::new
    );

    private final Expression field;

    @FunctionInfo(
        returnType = "boolean",
        preview = true,
        description = "Performs a match query on the specified field. Returns true if the provided query matches the row.",
        examples = { @Example(file = "match-function", tag = "match-with-field") }
    )
    public MatchFunction(
        Source source,
        @Param(name = "field", type = { "keyword", "text" }, description = "Field that the query will target.") Expression field,
        @Param(
            name = "query",
            type = { "keyword", "text" },
            description = "Text you wish to find in the provided field."
        ) Expression matchQuery
    ) {
        super(source, matchQuery, List.of(matchQuery, field));
        this.field = field;
    }

    private MatchFunction(StreamInput in) throws IOException {
        this(Source.readFrom((PlanStreamInput) in), in.readNamedWriteable(Expression.class), in.readNamedWriteable(Expression.class));
    }

    @Override
    public String functionName() {
        return "MATCH";
    }

    @Override
    public Query asQuery(String queryText) {
        return new MatchQuery(source(), ((FieldAttribute) field).name(), queryText);
    }

    @Override
    protected TypeResolution resolveNonQueryParamTypes() {
        return isNotNull(field, sourceText(), FIRST).and(isString(field, sourceText(), FIRST)).and(super.resolveNonQueryParamTypes());
    }

    @Override
    public void validate(Failures failures) {
        if (field instanceof FieldAttribute == false) {
            failures.add(
                Failure.fail(
                    field,
                    "[{}] cannot operate on [{}], which is not a field from an index mapping",
                    functionName(),
                    field.sourceText()
                )
            );
        }
    }

    @Override
    public Expression replaceChildren(List<Expression> newChildren) {
        // Query is the first child, field is the second child
        return new MatchFunction(source(), newChildren.get(1), newChildren.getFirst());
    }

    @Override
    protected NodeInfo<? extends Expression> info() {
        return NodeInfo.create(this, MatchFunction::new, field, query());
    }

    @Override
    public String getWriteableName() {
        return ENTRY.name;
    }

    protected TypeResolutions.ParamOrdinal queryParamOrdinal() {
        return SECOND;
    }

    @Override
    public boolean hasFieldsInformation() {
        return true;
    }
}
