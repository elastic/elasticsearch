/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.common.lucene.search;

import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexReaderContext;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.Term;
import org.apache.lucene.index.Terms;
import org.apache.lucene.index.TermsEnum;
import org.apache.lucene.queries.spans.SpanMultiTermQueryWrapper;
import org.apache.lucene.queries.spans.SpanOrQuery;
import org.apache.lucene.queries.spans.SpanQuery;
import org.apache.lucene.queries.spans.SpanTermQuery;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.MultiTermQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.util.AttributeSource;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.lucene.queries.SpanMatchNoDocsQuery;

import java.io.IOException;
import java.util.Collection;
import java.util.HashSet;
import java.util.Set;

/**
 * A span rewrite method that extracts the first <code>maxExpansions</code> terms
 * that match the {@link MultiTermQuery} in the terms dictionary.
 * The rewrite throws an error if more than <code>maxExpansions</code> terms are found and <code>hardLimit</code>
 * is set.
 */
public class SpanBooleanQueryRewriteWithMaxClause extends SpanMultiTermQueryWrapper.SpanRewriteMethod {
    private final int maxExpansions;
    private final boolean hardLimit;

    public SpanBooleanQueryRewriteWithMaxClause() {
        this(BooleanQuery.getMaxClauseCount(), true);
    }

    public SpanBooleanQueryRewriteWithMaxClause(int maxExpansions, boolean hardLimit) {
        this.maxExpansions = maxExpansions;
        this.hardLimit = hardLimit;
    }

    public int getMaxExpansions() {
        return maxExpansions;
    }

    public boolean isHardLimit() {
        return hardLimit;
    }

    @Override
    public SpanQuery rewrite(IndexReader reader, MultiTermQuery query) throws IOException {
        final MultiTermQuery.RewriteMethod delegate = new MultiTermQuery.RewriteMethod() {
            @Override
            public Query rewrite(IndexReader reader, MultiTermQuery query) throws IOException {
                Collection<SpanQuery> queries = collectTerms(reader, query);
                if (queries.size() == 0) {
                    return new SpanMatchNoDocsQuery(query.getField(), "no expansion found for " + query.toString());
                } else if (queries.size() == 1) {
                    return queries.iterator().next();
                } else {
                    return new SpanOrQuery(queries.toArray(new SpanQuery[0]));
                }
            }

            private Collection<SpanQuery> collectTerms(IndexReader reader, MultiTermQuery query) throws IOException {
                Set<SpanQuery> queries = new HashSet<>();
                IndexReaderContext topReaderContext = reader.getContext();
                for (LeafReaderContext context : topReaderContext.leaves()) {
                    final Terms terms = context.reader().terms(query.getField());
                    if (terms == null) {
                        // field does not exist
                        continue;
                    }

                    final TermsEnum termsEnum = getTermsEnum(query, terms, new AttributeSource());
                    assert termsEnum != null;

                    if (termsEnum == TermsEnum.EMPTY) {
                        continue;
                    }

                    BytesRef bytes;
                    while ((bytes = termsEnum.next()) != null) {
                        if (queries.size() >= maxExpansions) {
                            if (hardLimit) {
                                throw new RuntimeException(
                                    "["
                                        + query.toString()
                                        + " ] "
                                        + "exceeds maxClauseCount [ Boolean maxClauseCount is set to "
                                        + BooleanQuery.getMaxClauseCount()
                                        + "]"
                                );
                            } else {
                                return queries;
                            }
                        }
                        queries.add(new SpanTermQuery(new Term(query.getField(), bytes)));
                    }
                }
                return queries;
            }
        };
        return (SpanQuery) delegate.rewrite(reader, query);
    }
}
