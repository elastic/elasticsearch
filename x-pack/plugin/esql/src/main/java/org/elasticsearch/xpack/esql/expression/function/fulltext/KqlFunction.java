/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.expression.function.fulltext;

import org.apache.lucene.util.BytesRef;
import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.querydsl.query.Query;
import org.elasticsearch.xpack.esql.core.querydsl.query.QueryStringQuery;
import org.elasticsearch.xpack.esql.core.tree.NodeInfo;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.expression.function.Example;
import org.elasticsearch.xpack.esql.expression.function.FunctionInfo;
import org.elasticsearch.xpack.esql.expression.function.Param;
import org.elasticsearch.xpack.kql.query.KqlQueryBuilder;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Objects;

/**
 * Full text function that performs a {@link QueryStringQuery} .
 */
public class KqlFunction extends FullTextFunction {

    public static final NamedWriteableRegistry.Entry ENTRY = new NamedWriteableRegistry.Entry(
        Expression.class,
        "Kql",
        KqlFunction::new
    );

    @FunctionInfo(
        returnType = "boolean",
        preview = true,
        description = "Performs a kql query. Returns true if the provided kql query matches the row.",
        examples = { @Example(file = "kql-function", tag = "kql-simple-example") }
    )
    public KqlFunction(
        Source source,
        @Param(
            name = "query",
            type = { "keyword", "text" },
            description = "Query string in KQL."
        ) Expression queryString
    ) {
        super(source, queryString);
    }

    private KqlFunction(StreamInput in) throws IOException {
        super(in);
    }

    @Override
    public String functionName() {
        return "KQL";
    }

    @Override
    public Query asQuery() {
        Object queryAsObject = query().fold();
        if (queryAsObject instanceof BytesRef queryAsBytesRef) {
            return new QueryStringQuery(source(), queryAsBytesRef.utf8ToString(), Map.of(), null);
        } else {
            throw new IllegalArgumentException("Query in QSTR needs to be resolved to a string");
        }
    }

    @Override
    public Expression replaceChildren(List<Expression> newChildren) {
        return new KqlFunction(source(), newChildren.get(0));
    }

    @Override
    protected NodeInfo<? extends Expression> info() {
        return NodeInfo.create(this, KqlFunction::new, query());
    }

    @Override
    public String getWriteableName() {
        return ENTRY.name;
    }

    private static class KqlQuery extends Query {
        private final String query;

        private KqlQuery(Source source, String query) {
            super(source);
            this.query = query;
        }

        @Override
        public QueryBuilder asBuilder() {
            return new KqlQueryBuilder(query);
        }

        @Override
        public int hashCode() {
            return Objects.hash(query);
        }

        @Override
        public boolean equals(Object obj) {
            if (this == obj) {
                return true;
            }

            if (obj == null || getClass() != obj.getClass()) {
                return false;
            }

            KqlQuery other = (KqlQuery) obj;
            return Objects.equals(query, other.query);
        }

        @Override
        protected String innerToString() {
            return query;
        }
    }
}
