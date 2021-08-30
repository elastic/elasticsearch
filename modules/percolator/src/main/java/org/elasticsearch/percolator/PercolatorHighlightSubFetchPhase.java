/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.percolator;

import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.QueryVisitor;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.document.DocumentField;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.fetch.FetchContext;
import org.elasticsearch.search.fetch.FetchSubPhase;
import org.elasticsearch.search.fetch.FetchSubPhaseProcessor;
import org.elasticsearch.search.fetch.subphase.highlight.HighlightField;
import org.elasticsearch.search.fetch.subphase.highlight.HighlightPhase;
import org.elasticsearch.search.fetch.subphase.highlight.Highlighter;
import org.elasticsearch.search.fetch.subphase.highlight.SearchHighlightContext;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;

/**
 * Highlighting in the case of the percolate query is a bit different, because the PercolateQuery itself doesn't get highlighted,
 * but the source of the PercolateQuery gets highlighted by each hit containing a query.
 */
final class PercolatorHighlightSubFetchPhase implements FetchSubPhase {
    private final HighlightPhase highlightPhase;

    PercolatorHighlightSubFetchPhase(Map<String, Highlighter> highlighters) {
        this.highlightPhase = new HighlightPhase(highlighters);
    }

    @Override
    public String name() {
        return "percolator_highlight";
    }

    @Override
    public String description() {
        return "annotates text matching percolator queries";
    }

    @Override
    public FetchSubPhaseProcessor getProcessor(FetchContext fetchContext) {
        if (fetchContext.highlight() == null) {
            return null;
        }
        List<PercolateQuery> percolateQueries = locatePercolatorQuery(fetchContext.query());
        if (percolateQueries.isEmpty()) {
            return null;
        }
        return new FetchSubPhaseProcessor() {

            LeafReaderContext ctx;

            @Override
            public void setNextReader(LeafReaderContext readerContext) {
                this.ctx = readerContext;
            }

            @Override
            public void process(HitContext hit) throws IOException {
                boolean singlePercolateQuery = percolateQueries.size() == 1;
                for (PercolateQuery percolateQuery : percolateQueries) {
                    String fieldName = singlePercolateQuery ? PercolatorMatchedSlotSubFetchPhase.FIELD_NAME_PREFIX :
                        PercolatorMatchedSlotSubFetchPhase.FIELD_NAME_PREFIX + "_" + percolateQuery.getName();
                    IndexSearcher percolatorIndexSearcher = percolateQuery.getPercolatorIndexSearcher();
                    PercolateQuery.QueryStore queryStore = percolateQuery.getQueryStore();

                    LeafReaderContext percolatorLeafReaderContext = percolatorIndexSearcher.getIndexReader().leaves().get(0);
                    final Query query = queryStore.getQueries(ctx).apply(hit.docId());
                    if (query != null) {
                        DocumentField field = hit.hit().field(fieldName);
                        if (field == null) {
                            // It possible that a hit did not match with a particular percolate query,
                            // so then continue highlighting with the next hit.
                            continue;
                        }

                        for (Object matchedSlot : field.getValues()) {
                            int slot = (int) matchedSlot;
                            BytesReference document = percolateQuery.getDocuments().get(slot);
                            HitContext subContext = new HitContext(
                                new SearchHit(slot, "unknown", Collections.emptyMap(), Collections.emptyMap()),
                                percolatorLeafReaderContext,
                                slot);
                            subContext.sourceLookup().setSource(document);
                            // force source because MemoryIndex does not store fields
                            SearchHighlightContext highlight = new SearchHighlightContext(fetchContext.highlight().fields(), true);
                            FetchSubPhaseProcessor processor = highlightPhase.getProcessor(fetchContext, highlight, query);
                            processor.process(subContext);
                            for (Map.Entry<String, HighlightField> entry : subContext.hit().getHighlightFields().entrySet()) {
                                if (percolateQuery.getDocuments().size() == 1) {
                                    String hlFieldName;
                                    if (singlePercolateQuery) {
                                        hlFieldName = entry.getKey();
                                    } else {
                                        hlFieldName = percolateQuery.getName() + "_" + entry.getKey();
                                    }
                                    hit.hit().getHighlightFields().put(hlFieldName,
                                        new HighlightField(hlFieldName, entry.getValue().fragments()));
                                } else {
                                    // In case multiple documents are being percolated we need to identify to which document
                                    // a highlight belongs to.
                                    String hlFieldName;
                                    if (singlePercolateQuery) {
                                        hlFieldName = slot + "_" + entry.getKey();
                                    } else {
                                        hlFieldName = percolateQuery.getName() + "_" + slot + "_" + entry.getKey();
                                    }
                                    hit.hit().getHighlightFields().put(hlFieldName,
                                        new HighlightField(hlFieldName, entry.getValue().fragments()));
                                }
                            }
                        }
                    }
                }
            }
        };
    }

    static List<PercolateQuery> locatePercolatorQuery(Query query) {
        if (query == null) {
            return Collections.emptyList();
        }
        List<PercolateQuery> queries = new ArrayList<>();
        query.visit(new QueryVisitor() {
            @Override
            public void visitLeaf(Query query) {
                if (query instanceof PercolateQuery) {
                    queries.add((PercolateQuery)query);
                }
            }
        });
        return queries;
    }
}
