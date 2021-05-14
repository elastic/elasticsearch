/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
package org.elasticsearch.index.query;

import org.apache.lucene.queryparser.classic.QueryParser;
import org.apache.lucene.search.BooleanClause;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.util.CollectionUtils;

import java.io.IOException;
import java.util.Locale;

public enum Operator implements Writeable {
    OR, AND;

    public BooleanClause.Occur toBooleanClauseOccur() {
        switch (this) {
            case OR:
                return BooleanClause.Occur.SHOULD;
            case AND:
                return BooleanClause.Occur.MUST;
            default:
                throw Operator.newOperatorException(this.toString());
        }
    }

    public QueryParser.Operator toQueryParserOperator() {
        switch (this) {
            case OR:
                return QueryParser.Operator.OR;
            case AND:
                return QueryParser.Operator.AND;
            default:
                throw Operator.newOperatorException(this.toString());
        }
    }

    public static Operator readFromStream(StreamInput in) throws IOException {
        return in.readEnum(Operator.class);
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeEnum(this);
    }

    public static Operator fromString(String op) {
        return valueOf(op.toUpperCase(Locale.ROOT));
    }

    private static IllegalArgumentException newOperatorException(String op) {
        return new IllegalArgumentException("operator needs to be either " +
                CollectionUtils.arrayAsArrayList(values()) + ", but not [" + op + "]");
    }
}
