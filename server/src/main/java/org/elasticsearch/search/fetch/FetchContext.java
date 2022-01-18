/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.search.fetch;

import org.apache.lucene.search.Query;
import org.elasticsearch.index.query.ParsedQuery;
import org.elasticsearch.index.query.SearchExecutionContext;
import org.elasticsearch.search.SearchExtBuilder;
import org.elasticsearch.search.fetch.subphase.FetchDocValuesContext;
import org.elasticsearch.search.fetch.subphase.FetchFieldsContext;
import org.elasticsearch.search.fetch.subphase.FetchSourceContext;
import org.elasticsearch.search.fetch.subphase.FieldAndFormat;
import org.elasticsearch.search.fetch.subphase.InnerHitsContext;
import org.elasticsearch.search.fetch.subphase.InnerHitsContext.InnerHitSubContext;
import org.elasticsearch.search.fetch.subphase.ScriptFieldsContext;
import org.elasticsearch.search.fetch.subphase.highlight.SearchHighlightContext;
import org.elasticsearch.search.internal.ContextIndexSearcher;
import org.elasticsearch.search.internal.SearchContext;
import org.elasticsearch.search.lookup.SearchLookup;
import org.elasticsearch.search.lookup.SourceLookup;
import org.elasticsearch.search.rescore.RescoreContext;

import java.util.Collections;
import java.util.List;

/**
 * Encapsulates state required to execute fetch phases
 */
public class FetchContext {

    private final SearchContext searchContext;
    private final SearchLookup searchLookup;

    /**
     * Create a FetchContext based on a SearchContext
     */
    public FetchContext(SearchContext searchContext) {
        this.searchContext = searchContext;
        this.searchLookup = searchContext.getSearchExecutionContext().lookup();
    }

    /**
     * The name of the index that documents are being fetched from
     */
    public String getIndexName() {
        return searchContext.indexShard().shardId().getIndexName();
    }

    /**
     * The point-in-time searcher the original query was executed against
     */
    public ContextIndexSearcher searcher() {
        return searchContext.searcher();
    }

    /**
     * The {@code SearchLookup} for the this context
     */
    public SearchLookup searchLookup() {
        return searchLookup;
    }

    /**
     * The original query, not rewritten.
     */
    public Query query() {
        return searchContext.query();
    }

    /**
     * The original query in its rewritten form.
     */
    public Query rewrittenQuery() {
        return searchContext.rewrittenQuery();
    }

    /**
     * The original query with additional filters and named queries
     */
    public ParsedQuery parsedQuery() {
        return searchContext.parsedQuery();
    }

    /**
     * Any post-filters run as part of the search
     */
    public ParsedQuery parsedPostFilter() {
        return searchContext.parsedPostFilter();
    }

    /**
     * Configuration for fetching _source
     */
    public FetchSourceContext fetchSourceContext() {
        return searchContext.fetchSourceContext();
    }

    /**
     * Should the response include `explain` output
     */
    public boolean explain() {
        return searchContext.explain() && searchContext.query() != null;
    }

    /**
     * The rescorers included in the original search, used for explain output
     */
    public List<RescoreContext> rescore() {
        return searchContext.rescore();
    }

    /**
     * Should the response include sequence number and primary term metadata
     */
    public boolean seqNoAndPrimaryTerm() {
        return searchContext.seqNoAndPrimaryTerm();
    }

    /**
     * Configuration for fetching docValues fields
     */
    public FetchDocValuesContext docValuesContext() {
        FetchDocValuesContext dvContext = searchContext.docValuesContext();
        if (searchContext.collapse() != null) {
            // retrieve the `doc_value` associated with the collapse field
            String name = searchContext.collapse().getFieldName();
            if (dvContext == null) {
                return new FetchDocValuesContext(
                    searchContext.getSearchExecutionContext(),
                    Collections.singletonList(new FieldAndFormat(name, null))
                );
            } else if (searchContext.docValuesContext().fields().stream().map(ff -> ff.field).anyMatch(name::equals) == false) {
                dvContext.fields().add(new FieldAndFormat(name, null));
            }
        }
        return dvContext;
    }

    /**
     * Configuration for highlighting
     */
    public SearchHighlightContext highlight() {
        return searchContext.highlight();
    }

    /**
     * Does the index analyzer for this field have token filters that may produce
     * backwards offsets in term vectors
     */
    public boolean containsBrokenAnalysis(String field) {
        return getSearchExecutionContext().containsBrokenAnalysis(field);
    }

    /**
     * Should the response include scores, even if scores were not calculated in the original query
     */
    public boolean fetchScores() {
        return searchContext.sort() != null && searchContext.trackScores();
    }

    /**
     * Configuration for returning inner hits
     */
    public InnerHitsContext innerHits() {
        return searchContext.innerHits();
    }

    /**
     * Should the response include version metadata
     */
    public boolean version() {
        return searchContext.version();
    }

    /**
     * Configuration for the 'fields' response
     */
    public FetchFieldsContext fetchFieldsContext() {
        return searchContext.fetchFieldsContext();
    }

    /**
     * Configuration for script fields
     */
    public ScriptFieldsContext scriptFields() {
        return searchContext.scriptFields();
    }

    /**
     * Configuration for external fetch phase plugins
     */
    public SearchExtBuilder getSearchExt(String name) {
        return searchContext.getSearchExt(name);
    }

    public SearchExecutionContext getSearchExecutionContext() {
        return searchContext.getSearchExecutionContext();
    }

    /**
     * For a hit document that's being processed, return the source lookup representing the
     * root document. This method is used to pass down the root source when processing this
     * document's nested inner hits.
     *
     * @param hitContext The context of the hit that's being processed.
     */
    public SourceLookup getRootSourceLookup(FetchSubPhase.HitContext hitContext) {
        // Usually the root source simply belongs to the hit we're processing. But if
        // there are multiple layers of inner hits and we're in a nested context, then
        // the root source is found on the inner hits context.
        if (searchContext instanceof InnerHitSubContext innerHitsContext && hitContext.hit().getNestedIdentity() != null) {
            return innerHitsContext.getRootLookup();
        } else {
            return hitContext.sourceLookup();
        }
    }
}
