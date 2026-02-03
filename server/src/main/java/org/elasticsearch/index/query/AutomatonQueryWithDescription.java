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
import org.apache.lucene.util.automaton.Automaton;

/**
 * A specialized {@link AutomatonQuery} that includes a description of the query.
 * This can be useful for debugging or logging purposes, providing more context
 * about the query being executed.
 */
public class AutomatonQueryWithDescription extends AutomatonQuery {
    private final String description;

    public AutomatonQueryWithDescription(Term term, Automaton automaton, String description) {
        super(term, automaton);
        this.description = description;
    }

    @Override
    public String toString(String field) {
        if (this.field.equals(field)) {
            return description;
        }
        return this.field + ":" + description;
    }
}
