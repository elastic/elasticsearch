/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.sql.expression.predicate.fulltext;

import java.util.Objects;

import org.elasticsearch.xpack.sql.expression.Expression;
import org.elasticsearch.xpack.sql.tree.Location;

import static java.util.Collections.singletonList;

public class MatchQueryPredicate extends FullTextPredicate {

    private final Expression field;
    private final Operator operator;
    
    public MatchQueryPredicate(Location location, Expression field, String query, String options) {
        super(location, query, options, singletonList(field));
        this.field = field;

        this.operator = FullTextUtils.operator(optionMap(), "operator");
    }

    public Expression field() {
        return field;
    }

    public Operator operator() {
        return operator;
    }

    @Override
    public int hashCode() {
        return Objects.hash(field, super.hashCode());
    }

    @Override
    public boolean equals(Object obj) {
        if (super.equals(obj)) {
            MatchQueryPredicate other = (MatchQueryPredicate) obj;
            return Objects.equals(field, other.field);
        }
        return false;
    }
}
