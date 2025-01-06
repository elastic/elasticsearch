/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.expression.function.fulltext;

import org.apache.lucene.util.BytesRef;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.expression.Nullability;
import org.elasticsearch.xpack.esql.core.expression.TranslationAware;
import org.elasticsearch.xpack.esql.core.expression.TypeResolutions;
import org.elasticsearch.xpack.esql.core.expression.function.Function;
import org.elasticsearch.xpack.esql.core.planner.ExpressionTranslator;
import org.elasticsearch.xpack.esql.core.planner.TranslatorHandler;
import org.elasticsearch.xpack.esql.core.querydsl.query.Query;
import org.elasticsearch.xpack.esql.core.querydsl.query.TranslationAwareExpressionQuery;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.type.DataType;

import java.util.List;
import java.util.Objects;

import static org.elasticsearch.xpack.esql.core.expression.TypeResolutions.ParamOrdinal.DEFAULT;
import static org.elasticsearch.xpack.esql.core.expression.TypeResolutions.isNotNullAndFoldable;
import static org.elasticsearch.xpack.esql.core.expression.TypeResolutions.isString;

/**
 * Base class for full-text functions that use ES queries to match documents.
 * These functions needs to be pushed down to Lucene queries to be executed - there's no Evaluator for them, but depend on
 * {@link org.elasticsearch.xpack.esql.optimizer.LocalPhysicalPlanOptimizer} to rewrite them into Lucene queries.
 */
public abstract class FullTextFunction extends Function implements TranslationAware {

    private final Expression query;
    private final QueryBuilder queryBuilder;

    protected FullTextFunction(Source source, Expression query, List<Expression> children, QueryBuilder queryBuilder) {
        super(source, children);
        this.query = query;
        this.queryBuilder = queryBuilder;
    }

    @Override
    public DataType dataType() {
        return DataType.BOOLEAN;
    }

    @Override
    protected final TypeResolution resolveType() {
        if (childrenResolved() == false) {
            return new TypeResolution("Unresolved children");
        }

        return resolveNonQueryParamTypes().and(resolveQueryParamType().and(checkParamCompatibility()));
    }

    /**
     * Checks parameter specific compatibility, to be overriden by subclasses
     *
     * @return TypeResolution for param compatibility
     */
    protected TypeResolution checkParamCompatibility() {
        return TypeResolution.TYPE_RESOLVED;
    }

    /**
     * Resolves the type for the query parameter, as part of the type resolution for the function
     *
     * @return type resolution for query parameter
     */
    protected TypeResolution resolveQueryParamType() {
        return isString(query(), sourceText(), queryParamOrdinal()).and(isNotNullAndFoldable(query(), sourceText(), queryParamOrdinal()));
    }

    /**
     * Subclasses can override this method for custom type resolution for additional function parameters
     *
     * @return type resolution for non-query parameter types
     */
    protected TypeResolution resolveNonQueryParamTypes() {
        return TypeResolution.TYPE_RESOLVED;
    }

    public Expression query() {
        return query;
    }

    /**
     * Returns the resulting query as an object
     *
     * @return query expression as an object
     */
    public Object queryAsObject() {
        Object queryAsObject = query().fold();
        if (queryAsObject instanceof BytesRef bytesRef) {
            return bytesRef.utf8ToString();
        }

        return queryAsObject;
    }

    /**
     * Returns the param ordinal for the query parameter so it can be used in error messages
     *
     * @return Query ordinal for the
     */
    protected TypeResolutions.ParamOrdinal queryParamOrdinal() {
        return DEFAULT;
    }

    @Override
    public Nullability nullable() {
        return Nullability.FALSE;
    }

    /**
     * Used to differentiate error messages between functions and operators
     *
     * @return function type for error messages
     */
    public String functionType() {
        return "function";
    }

    @Override
    public int hashCode() {
        return Objects.hash(super.hashCode(), queryBuilder);
    }

    @Override
    public boolean equals(Object obj) {
        if (false == super.equals(obj)) {
            return false;
        }

        return Objects.equals(queryBuilder, ((FullTextFunction) obj).queryBuilder);
    }

    @Override
    public Query asQuery(TranslatorHandler translatorHandler) {
        if (queryBuilder != null) {
            return new TranslationAwareExpressionQuery(source(), queryBuilder);
        }

        ExpressionTranslator<? extends FullTextFunction> translator = translator();
        return translator.translate(this, translatorHandler);
    }

    public QueryBuilder queryBuilder() {
        return queryBuilder;
    }

    @SuppressWarnings("rawtypes")
    protected abstract ExpressionTranslator<? extends FullTextFunction> translator();

    public abstract Expression replaceQueryBuilder(QueryBuilder queryBuilder);
}
