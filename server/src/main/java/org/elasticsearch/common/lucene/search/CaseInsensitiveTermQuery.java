/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.common.lucene.search;

import org.apache.lucene.index.Term;
import org.apache.lucene.search.AutomatonQuery;

import static org.elasticsearch.common.lucene.search.AutomatonQueries.toCaseInsensitiveString;

/**
 * A case insensitive term query.
 */
public class CaseInsensitiveTermQuery extends AutomatonQuery {
    /**
     * Constructs a case insensitive term query.
     * @param term the term to search for, created into a case insensitive automaton
     */
    public CaseInsensitiveTermQuery(Term term) {
        super(term, toCaseInsensitiveString(term.bytes()));
    }

    @Override
    public String toString(String field) {
        StringBuilder buffer = new StringBuilder();
        buffer.append(getClass().getSimpleName());
        buffer.append('{');
        if (term.field().equals(field) == false) {
            buffer.append(term.field());
            buffer.append(':');
        }
        buffer.append(term.text());
        buffer.append('}');
        return buffer.toString();
    }
}
