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

import static org.elasticsearch.common.lucene.search.AutomatonQueries.toCaseInsensitiveWildcardAutomaton;

/**
 * A case insensitive wildcard query.
 */
public class CaseInsensitiveWildcardQuery extends AutomatonQuery {
    /**
     * Constructs a case insensitive wildcard query.
     * @param term the term to search for, created into a case insensitive wildcard automaton
     */
    public CaseInsensitiveWildcardQuery(Term term) {
        super(term, toCaseInsensitiveWildcardAutomaton(term));
    }

    public CaseInsensitiveWildcardQuery(Term term, boolean isBinary, RewriteMethod rewriteMethod) {
        super(term, toCaseInsensitiveWildcardAutomaton(term), isBinary, rewriteMethod);
    }

    @Override
    public String toString(String field) {
        return this.getClass().getSimpleName() + "{" + field + ":" + term.text() + "}";
    }
}
