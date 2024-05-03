/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.common.lucene.search;

import org.apache.lucene.index.Term;
import org.apache.lucene.search.AutomatonQuery;
import org.apache.lucene.search.MultiTermQuery;

import static org.elasticsearch.common.lucene.search.AutomatonQueries.caseInsensitivePrefix;

public class CaseInsensitivePrefixQuery extends AutomatonQuery {
    public CaseInsensitivePrefixQuery(Term term) {
        super(term, caseInsensitivePrefix(term.text()));
    }

    public CaseInsensitivePrefixQuery(Term term, int determinizeWorkLimit, boolean isBinary) {
        super(term, caseInsensitivePrefix(term.text()), determinizeWorkLimit, isBinary);
    }

    public CaseInsensitivePrefixQuery(Term term, int determinizeWorkLimit, boolean isBinary, MultiTermQuery.RewriteMethod rewriteMethod) {
        super(term, caseInsensitivePrefix(term.text()), determinizeWorkLimit, isBinary, rewriteMethod);
    }

    @Override
    public String toString(String field) {
        return this.getClass().getSimpleName() + "{" + field + ":" + term.text() + "}";
    }
}
