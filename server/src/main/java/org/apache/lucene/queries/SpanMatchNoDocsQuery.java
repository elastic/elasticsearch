package org.apache.lucene.queries;

import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.Term;
import org.apache.lucene.index.TermStates;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.ScoreMode;
import org.apache.lucene.search.spans.SpanQuery;
import org.apache.lucene.search.spans.SpanWeight;
import org.apache.lucene.search.spans.Spans;

import java.io.IOException;
import java.util.Collections;
import java.util.Map;
import java.util.Set;

/**
 * A {@link SpanQuery} that matches no documents.
 */
public class SpanMatchNoDocsQuery extends SpanQuery {
    private final String reason;

    public SpanMatchNoDocsQuery(String reason) {
        this.reason = reason;
    }

    @Override
    public String getField() {
        return null;
    }

    @Override
    public String toString(String field) {
        return "SpanMatchNoDocsQuery(\"" + reason + "\")";
    }

    @Override
    public boolean equals(Object o) {
        return sameClassAs(o);
    }

    @Override
    public int hashCode() {
        return classHash();
    }

    @Override
    public SpanWeight createWeight(IndexSearcher searcher, ScoreMode scoreMode, float boost) throws IOException {
        return new SpanWeight(this, searcher, Collections.emptyMap(), boost) {
            @Override
            public void extractTermStates(Map<Term, TermStates> contexts) {}

            @Override
            public Spans getSpans(LeafReaderContext ctx, Postings requiredPostings) {
                return null;
            }

            @Override
            public void extractTerms(Set<Term> terms) {}

            @Override
            public boolean isCacheable(LeafReaderContext ctx) {
                return true;
            }
        };
    }
}
