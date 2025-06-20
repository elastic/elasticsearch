/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.query;

import org.apache.lucene.index.Term;
import org.apache.lucene.search.AutomatonQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.util.automaton.Automaton;
import org.elasticsearch.TransportVersion;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.xcontent.XContentBuilder;

import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.util.Objects;

/**
 * Implements an Automaton query, which matches documents based on a Lucene Automaton.
 * It does not support serialization or XContent representation,
 */
public class AutomatonQueryBuilder extends AbstractQueryBuilder<AutomatonQueryBuilder> implements MultiTermQueryBuilder {
    private final String fieldName;
    private final Automaton automaton;
    private final String description;

    public AutomatonQueryBuilder(String fieldName, Automaton automaton, String description) {
        this.description = description;
        if (Strings.isEmpty(fieldName)) {
            throw new IllegalArgumentException("field name is null or empty");
        }
        if (automaton == null) {
            throw new IllegalArgumentException("automaton cannot be null");
        }
        this.fieldName = fieldName;
        this.automaton = automaton;
    }

    @Override
    public String fieldName() {
        return fieldName;
    }

    @Override
    public String getWriteableName() {
        throw new UnsupportedOperationException("AutomatonQueryBuilder does not support getWriteableName");
    }

    @Override
    protected void doWriteTo(StreamOutput out) throws IOException {
        throw new UnsupportedEncodingException("AutomatonQueryBuilder does not support doWriteTo");
    }

    @Override
    protected void doXContent(XContentBuilder builder, Params params) throws IOException {
        throw new UnsupportedEncodingException("AutomatonQueryBuilder does not support doXContent");
    }

    @Override
    protected Query doToQuery(SearchExecutionContext context) throws IOException {
        return new AutomatonQueryWithDescription(new Term(fieldName), automaton, description);
    }

    @Override
    protected int doHashCode() {
        return Objects.hash(fieldName, automaton);
    }

    @Override
    protected boolean doEquals(AutomatonQueryBuilder other) {
        return Objects.equals(fieldName, other.fieldName) && Objects.equals(automaton, other.automaton);
    }

    @Override
    public TransportVersion getMinimalSupportedVersion() {
        throw new UnsupportedOperationException("AutomatonQueryBuilder does not support getMinimalSupportedVersion");
    }

    static class AutomatonQueryWithDescription extends AutomatonQuery {
        private final String description;

        AutomatonQueryWithDescription(Term term, Automaton automaton, String description) {
            super(term, automaton);
            this.description = description;
        }

        @Override
        public String toString(String field) {
            if(description.isEmpty()) {
                return super.toString(field);
            }
            return description;
        }
    }
}
