/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.elasticsearch.common.lucene.search;

import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexReaderContext;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.Term;
import org.apache.lucene.index.Terms;
import org.apache.lucene.index.TermsEnum;
import org.apache.lucene.queries.SpanMatchNoDocsQuery;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.MultiTermQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.spans.SpanMultiTermQueryWrapper;
import org.apache.lucene.search.spans.SpanOrQuery;
import org.apache.lucene.search.spans.SpanQuery;
import org.apache.lucene.search.spans.SpanTermQuery;
import org.apache.lucene.util.BytesRef;

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

                    final TermsEnum termsEnum = getTermsEnum(query, terms, null);
                    assert termsEnum != null;

                    if (termsEnum == TermsEnum.EMPTY)
                        continue;

                    BytesRef bytes;
                    while ((bytes = termsEnum.next()) != null) {
                        if (queries.size() >= maxExpansions) {
                            if (hardLimit) {
                                throw new RuntimeException("[" + query.toString() + " ] " +
                                    "exceeds maxClauseCount [ Boolean maxClauseCount is set to " + BooleanQuery.getMaxClauseCount() + "]");
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
