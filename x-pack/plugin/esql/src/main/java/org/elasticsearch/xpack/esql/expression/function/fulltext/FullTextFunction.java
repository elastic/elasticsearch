/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.expression.function.fulltext;

import org.apache.lucene.util.BytesRef;
import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.xpack.esql.action.EsqlCapabilities;
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.expression.TypeResolutions;
import org.elasticsearch.xpack.esql.core.expression.function.Function;
import org.elasticsearch.xpack.esql.core.querydsl.query.Query;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.type.DataType;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import static org.elasticsearch.common.logging.LoggerMessageFormat.format;
import static org.elasticsearch.xpack.esql.core.expression.TypeResolutions.ParamOrdinal.DEFAULT;
import static org.elasticsearch.xpack.esql.core.expression.TypeResolutions.isNotNullAndFoldable;
import static org.elasticsearch.xpack.esql.core.expression.TypeResolutions.isString;

/**
 * Base class for full-text functions that use ES queries to match documents.
 * These functions needs to be pushed down to Lucene queries to be executed - there's no Evaluator for them, but depend on
 * {@link org.elasticsearch.xpack.esql.optimizer.LocalPhysicalPlanOptimizer} to rewrite them into Lucene queries.
 */
public abstract class FullTextFunction extends Function {
    public static List<NamedWriteableRegistry.Entry> getNamedWriteables() {
        List<NamedWriteableRegistry.Entry> entries = new ArrayList<>();
        if (EsqlCapabilities.Cap.QSTR_FUNCTION.isEnabled()) {
            entries.add(QueryStringFunction.ENTRY);
        }
        if (EsqlCapabilities.Cap.MATCH_FUNCTION.isEnabled()) {
            entries.add(MatchFunction.ENTRY);
        }
        return entries;
    }

    private final Expression query;

    protected FullTextFunction(Source source, Expression query, List<Expression> children) {
        super(source, children);
        this.query = query;
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

        return resolveNonQueryParamTypes().and(resolveQueryParamType());
    }

    /**
     * Resolves the type for the query parameter, as part of the type resolution for the function
     *
     * @return type resolution for query parameter
     */
    private TypeResolution resolveQueryParamType() {
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
     * Returns the resulting Query for the function parameters so it can be pushed down to Lucene
     *
     * @return Lucene query
     */
    public final Query asQuery() {
        Object queryAsObject = query().fold();
        if (queryAsObject instanceof BytesRef bytesRef) {
            return asQuery(bytesRef.utf8ToString());
        }

        throw new IllegalArgumentException(
            format(null, "{} argument in {} function needs to be resolved to a string", queryParamOrdinal(), functionName())
        );
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        source().writeTo(out);
        out.writeNamedWriteable(query());
        out.writeNamedWriteableCollection(children());
    }

    /**
     * Overriden by subclasses to return the corresponding Lucene query from a query text
     *
     * @return corresponding query for the query text
     */
    protected abstract Query asQuery(String queryText);

    /**
     * Returns the param ordinal for the query parameter so it can be used in error messages
     *
     * @return Query ordinal for the
     */
    protected TypeResolutions.ParamOrdinal queryParamOrdinal() {
        return DEFAULT;
    }

    public abstract boolean hasFieldsInformation();
}
