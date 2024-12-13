/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.expression.function.fulltext;

import org.elasticsearch.TransportVersions;
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
import org.elasticsearch.xpack.esql.core.planner.ExpressionTranslator;
import org.elasticsearch.xpack.esql.core.querydsl.query.TermQuery;
import org.elasticsearch.xpack.esql.core.tree.NodeInfo;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.expression.function.Example;
import org.elasticsearch.xpack.esql.expression.function.FunctionInfo;
import org.elasticsearch.xpack.esql.expression.function.Param;
import org.elasticsearch.xpack.esql.io.stream.PlanStreamInput;
import org.elasticsearch.xpack.esql.planner.EsqlExpressionTranslators;

import java.io.IOException;
import java.util.List;

import static org.elasticsearch.xpack.esql.core.expression.TypeResolutions.ParamOrdinal.FIRST;
import static org.elasticsearch.xpack.esql.core.expression.TypeResolutions.ParamOrdinal.SECOND;
import static org.elasticsearch.xpack.esql.core.expression.TypeResolutions.isNotNull;
import static org.elasticsearch.xpack.esql.core.expression.TypeResolutions.isString;

/**
 * Full text function that performs a {@link TermQuery} .
 */
public class Term extends FullTextFunction implements Validatable {

    public static final NamedWriteableRegistry.Entry ENTRY = new NamedWriteableRegistry.Entry(Expression.class, "Term", Term::readFrom);

    private final Expression field;

    @FunctionInfo(
        returnType = "boolean",
        preview = true,
        description = "Performs a Term query on the specified field. Returns true if the provided term matches the row.",
        examples = { @Example(file = "term-function", tag = "term-with-field") }
    )
    public Term(
        Source source,
        @Param(name = "field", type = { "keyword", "text" }, description = "Field that the query will target.") Expression field,
        @Param(
            name = "query",
            type = { "keyword", "text" },
            description = "Term you wish to find in the provided field."
        ) Expression termQuery
    ) {
        this(source, field, termQuery, null);
    }

    public Term(Source source, Expression field, Expression termQuery, QueryBuilder queryBuilder) {
        super(source, termQuery, List.of(field, termQuery), queryBuilder);
        this.field = field;
    }

    private static Term readFrom(StreamInput in) throws IOException {
        Source source = Source.readFrom((PlanStreamInput) in);
        Expression field = in.readNamedWriteable(Expression.class);
        Expression query = in.readNamedWriteable(Expression.class);
        QueryBuilder queryBuilder = null;
        if (in.getTransportVersion().onOrAfter(TransportVersions.ESQL_QUERY_BUILDER_IN_SEARCH_FUNCTIONS)) {
            queryBuilder = in.readOptionalNamedWriteable(QueryBuilder.class);
        }
        return new Term(source, field, query, queryBuilder);
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        source().writeTo(out);
        out.writeNamedWriteable(field());
        out.writeNamedWriteable(query());
        if (out.getTransportVersion().onOrAfter(TransportVersions.ESQL_QUERY_BUILDER_IN_SEARCH_FUNCTIONS)) {
            out.writeOptionalNamedWriteable(queryBuilder());
        }
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
        return new Term(source(), newChildren.get(0), newChildren.get(1), queryBuilder());
    }

    @Override
    protected NodeInfo<? extends Expression> info() {
        return NodeInfo.create(this, Term::new, field, query(), queryBuilder());
    }

    protected TypeResolutions.ParamOrdinal queryParamOrdinal() {
        return SECOND;
    }

    @Override
    protected ExpressionTranslator<Term> translator() {
        return new EsqlExpressionTranslators.TermFunctionTranslator();
    }

    @Override
    public Expression replaceQueryBuilder(QueryBuilder queryBuilder) {
        return new Term(source(), field, query(), queryBuilder);
    }

    public Expression field() {
        return field;
    }

    @Override
    public String functionName() {
        return ENTRY.name;
    }
}
