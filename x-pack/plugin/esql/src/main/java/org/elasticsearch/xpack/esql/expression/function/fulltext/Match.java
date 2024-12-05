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
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.xpack.esql.capabilities.Validatable;
import org.elasticsearch.xpack.esql.common.Failure;
import org.elasticsearch.xpack.esql.common.Failures;
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.expression.FieldAttribute;
import org.elasticsearch.xpack.esql.core.expression.TypeResolutions;
import org.elasticsearch.xpack.esql.core.querydsl.query.ChickenQueryBuilder;
import org.elasticsearch.xpack.esql.core.querydsl.query.MatchQuery;
import org.elasticsearch.xpack.esql.core.querydsl.query.Query;
import org.elasticsearch.xpack.esql.core.querydsl.query.QueryStringQuery;
import org.elasticsearch.xpack.esql.core.querydsl.query.ResolvedQueryBuilder;
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
import java.util.Objects;

import static org.elasticsearch.xpack.esql.core.expression.TypeResolutions.ParamOrdinal.FIRST;
import static org.elasticsearch.xpack.esql.core.expression.TypeResolutions.ParamOrdinal.SECOND;
import static org.elasticsearch.xpack.esql.core.expression.TypeResolutions.isNotNull;
import static org.elasticsearch.xpack.esql.core.expression.TypeResolutions.isString;

/**
 * Full text function that performs a {@link QueryStringQuery} .
 */
public class Match extends FullTextFunction implements Validatable {

    public static final NamedWriteableRegistry.Entry ENTRY = new NamedWriteableRegistry.Entry(Expression.class, "Match", Match::readFrom);

    private final Expression field;
    private final QueryBuilder queryBuilder;

    private transient Boolean isOperator;

    // TODO: I dislike the need of having a private constructor here
    // We cannot simply add the queryBuilder argument to the existing constructor unless we
    // add new FunctionDefinitions in EsqlFunctionRegistry to account for functions that can receive
    // a QueryBuilder argument. This would be similar to what we did for the functions that can receive
    // Configuration (e.g. Now).
    private Match(Source source, Expression field, Expression matchQuery, QueryBuilder queryBuilder) {
        super(source, matchQuery, List.of(field, matchQuery));
        this.field = field;
        this.queryBuilder = queryBuilder;
    }

    @FunctionInfo(
        returnType = "boolean",
        preview = true,
        description = "Performs a match query on the specified field. Returns true if the provided query matches the row.",
        examples = { @Example(file = "match-function", tag = "match-with-field") }
    )
    public Match(
        Source source,
        @Param(name = "field", type = { "keyword", "text" }, description = "Field that the query will target.") Expression field,
        @Param(
            name = "query",
            type = { "keyword", "text" },
            description = "Text you wish to find in the provided field."
        ) Expression matchQuery
    ) {
        this(source, field, matchQuery, null);
    }

    public Match newWithQueryBuilder(QueryBuilder queryBuilder) {
        return new Match(source(), field, query(), queryBuilder);
    }

    public Query asQuery() {
        if (queryBuilder != null) {
            return new ResolvedQueryBuilder(source(), queryBuilder);
        }

        // special temp handling of semantic text to show it works
        if (field.dataType() == DataType.SEMANTIC_TEXT) {
            return new ResolvedQueryBuilder(
                source(),
                new ChickenQueryBuilder(((FieldAttribute) field).name(), queryAsText())
            );
        }
        // end special handling of semantic text

        return new MatchQuery(source(), ((FieldAttribute) field).name(), queryAsText());
    }

    private static Match readFrom(StreamInput in) throws IOException {
        Source source = Source.readFrom((PlanStreamInput) in);
        Expression field = in.readNamedWriteable(Expression.class);
        Expression query = in.readNamedWriteable(Expression.class);
        QueryBuilder queryBuilder = in.readNamedWriteable(QueryBuilder.class);
        return new Match(source, field, query, queryBuilder);
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        source().writeTo(out);
        out.writeNamedWriteable(field());
        out.writeNamedWriteable(query());
        out.writeNamedWriteable(queryBuilder);
    }

    @Override
    public String getWriteableName() {
        return ENTRY.name;
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
        return new Match(source(), newChildren.get(0), newChildren.get(1), queryBuilder);
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

    public QueryBuilder queryBuilder() {
        return queryBuilder;
    }

    private boolean isOperator() {
        if (isOperator == null) {
            isOperator = source().text().toUpperCase(Locale.ROOT).matches("^" + super.functionName() + "\\s*\\(.*\\)") == false;
        }
        return isOperator;
    }

    @Override
    public int hashCode() {
        return Objects.hash(super.hashCode(), field, queryBuilder);
    }

    @Override
    public boolean equals(Object obj) {
        if (super.equals(obj) == false) {
            return false;
        }

        Match other = (Match) obj;
        return functionType().equals(other.functionType())
            && field.equals(other.field)
            && query().equals(other.query())
            && Objects.equals(queryBuilder, other.queryBuilder);
    }
}
