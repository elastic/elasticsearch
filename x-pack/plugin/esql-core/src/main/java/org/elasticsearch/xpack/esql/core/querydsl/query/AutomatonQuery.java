/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.esql.core.querydsl.query;

import org.apache.lucene.util.automaton.Automaton;
import org.elasticsearch.index.query.AutomatonQueryBuilder;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.xpack.esql.core.tree.Source;

import java.util.Objects;

/**
 * Query that matches documents based on a Lucene Automaton.
 */
public class AutomatonQuery extends Query {

    private final String field;
    private final Automaton automaton;

    public AutomatonQuery(Source source, String field, Automaton automaton) {
        super(source);
        this.field = field;
        this.automaton = automaton;
    }

    public String field() {
        return field;
    }

    @Override
    protected QueryBuilder asBuilder() {
        return new AutomatonQueryBuilder(field, automaton);
    }

    @Override
    public int hashCode() {
        return Objects.hash(field, automaton);
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }

        if (obj == null || getClass() != obj.getClass()) {
            return false;
        }

        AutomatonQuery other = (AutomatonQuery) obj;
        return Objects.equals(field, other.field) && Objects.equals(automaton, other.automaton);
    }

    @Override
    protected String innerToString() {
        return "AutomatonQuery{" + "field='" + field + '\'' + '}';
    }
}
