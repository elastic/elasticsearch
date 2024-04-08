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

import java.io.IOException;
import java.util.Locale;

public enum Operator implements Writeable {
    OR,
    AND;

    public BooleanClause.Occur toBooleanClauseOccur() {
        return switch (this) {
            case OR -> BooleanClause.Occur.SHOULD;
            case AND -> BooleanClause.Occur.MUST;
        };
    }

    public QueryParser.Operator toQueryParserOperator() {
        return switch (this) {
            case OR -> QueryParser.Operator.OR;
            case AND -> QueryParser.Operator.AND;
        };
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

}
