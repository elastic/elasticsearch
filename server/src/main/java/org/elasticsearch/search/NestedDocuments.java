/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.search;

import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.ReaderUtil;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.ScoreMode;
import org.apache.lucene.search.Scorer;
import org.apache.lucene.search.Weight;
import org.apache.lucene.search.join.BitSetProducer;
import org.apache.lucene.util.BitSet;
import org.elasticsearch.common.lucene.search.Queries;
import org.elasticsearch.index.IndexVersion;
import org.elasticsearch.index.mapper.MappingLookup;
import org.elasticsearch.index.mapper.NestedLookup;
import org.elasticsearch.index.mapper.NestedObjectMapper;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.function.Function;

/**
 * Manages loading information about nested documents
 */
public class NestedDocuments {

    private final Map<String, BitSetProducer> parentObjectFilters = new HashMap<>();
    private final Map<String, Weight> childObjectFilters = new HashMap<>();
    private final BitSetProducer parentDocumentFilter;
    private final NestedLookup nestedLookup;

    /**
     * Create a new NestedDocuments object for an index
     * @param mappingLookup     the index's mapping
     * @param filterProducer    a function to build BitSetProducers from filter queries
     * @param indexVersionCreated the index creation version
     */
    public NestedDocuments(MappingLookup mappingLookup, Function<Query, BitSetProducer> filterProducer, IndexVersion indexVersionCreated) {
        this.nestedLookup = mappingLookup.nestedLookup();
        if (this.nestedLookup == NestedLookup.EMPTY) {
            this.parentDocumentFilter = null;
        } else {
            this.parentDocumentFilter = filterProducer.apply(Queries.newNonNestedFilter(indexVersionCreated));
            nestedLookup.getNestedParentFilters().forEach((k, v) -> parentObjectFilters.put(k, filterProducer.apply(v)));
            for (String nestedPath : nestedLookup.getNestedMappers().keySet()) {
                childObjectFilters.put(nestedPath, null);
            }
        }
    }

    /**
     * Returns a LeafNestedDocuments for an index segment
     */
    public LeafNestedDocuments getLeafNestedDocuments(LeafReaderContext ctx) throws IOException {
        if (parentDocumentFilter == null) {
            return LeafNestedDocuments.NO_NESTED_MAPPERS;
        }
        return new HasNestedDocuments(ctx);
    }

    private Weight getNestedChildWeight(LeafReaderContext ctx, String path) throws IOException {
        if (childObjectFilters.containsKey(path) == false) {
            throw new IllegalStateException("Cannot find object mapper for path " + path);
        }
        if (childObjectFilters.get(path) == null) {
            IndexSearcher searcher = new IndexSearcher(ReaderUtil.getTopLevelContext(ctx));
            NestedObjectMapper childMapper = nestedLookup.getNestedMappers().get(path);
            childObjectFilters.put(
                path,
                searcher.createWeight(searcher.rewrite(childMapper.nestedTypeFilter()), ScoreMode.COMPLETE_NO_SCORES, 1)
            );
        }
        return childObjectFilters.get(path);
    }

    private class HasNestedDocuments implements LeafNestedDocuments {

        final LeafReaderContext ctx;
        final BitSet parentFilter;
        final Map<String, BitSet> objectFilters = new HashMap<>();
        final Map<String, Scorer> childScorers = new HashMap<>();

        int doc = -1;
        int rootDoc = -1;
        SearchHit.NestedIdentity nestedIdentity = null;

        private HasNestedDocuments(LeafReaderContext ctx) throws IOException {
            this.ctx = ctx;
            this.parentFilter = parentDocumentFilter.getBitSet(ctx);
            for (Map.Entry<String, BitSetProducer> filter : parentObjectFilters.entrySet()) {
                BitSet bits = filter.getValue().getBitSet(ctx);
                if (bits != null) {
                    objectFilters.put(filter.getKey(), bits);
                }
            }
            for (Map.Entry<String, Weight> childFilter : childObjectFilters.entrySet()) {
                Scorer scorer = getNestedChildWeight(ctx, childFilter.getKey()).scorer(ctx);
                if (scorer != null) {
                    childScorers.put(childFilter.getKey(), scorer);
                }
            }
        }

        @Override
        public SearchHit.NestedIdentity advance(int doc) throws IOException {
            assert doc >= 0 && doc < ctx.reader().maxDoc();
            if (parentFilter.get(doc)) {
                // parent doc, no nested identity
                this.nestedIdentity = null;
                this.doc = doc;
                this.rootDoc = doc;
                return null;
            } else {
                this.doc = doc;
                this.rootDoc = parentFilter.nextSetBit(doc);
                return this.nestedIdentity = loadNestedIdentity();
            }
        }

        @Override
        public int doc() {
            assert doc != -1 : "Called doc() when unpositioned";
            return doc;
        }

        @Override
        public int rootDoc() {
            assert doc != -1 : "Called rootDoc() when unpositioned";
            return rootDoc;
        }

        @Override
        public SearchHit.NestedIdentity nestedIdentity() {
            assert doc != -1 : "Called nestedIdentity() when unpositioned";
            return nestedIdentity;
        }

        private String findObjectPath(int doc) throws IOException {
            String path = null;
            for (Map.Entry<String, Scorer> objectFilter : childScorers.entrySet()) {
                DocIdSetIterator it = objectFilter.getValue().iterator();
                if (it.docID() == doc || it.docID() < doc && it.advance(doc) == doc) {
                    if (path == null || path.length() > objectFilter.getKey().length()) {
                        path = objectFilter.getKey();
                    }
                }
            }
            if (path == null) {
                throw new IllegalStateException("Cannot find object path for document " + doc);
            }
            return path;
        }

        private SearchHit.NestedIdentity loadNestedIdentity() throws IOException {
            SearchHit.NestedIdentity ni = null;
            int currentLevelDoc = doc;
            int parentNameLength;
            String path = findObjectPath(doc);
            while (path != null) {
                String parent = nestedLookup.getNestedParent(path);
                // We have to pull a new scorer for each document here, because we advance from
                // the last parent which will be behind the doc
                Scorer childScorer = getNestedChildWeight(ctx, path).scorer(ctx);
                if (childScorer == null) {
                    throw new IllegalStateException("Cannot find object mapper for path " + path + " in doc " + doc);
                }
                BitSet parentBitSet;
                if (parent == null) {
                    parentBitSet = parentFilter;
                    parentNameLength = 0;
                } else {
                    if (objectFilters.containsKey(parent) == false) {
                        throw new IllegalStateException(
                            "Cannot find parent mapper "
                                + parent
                                + " for path "
                                + path
                                + " in doc "
                                + doc
                                + " - known parents are "
                                + objectFilters.keySet()
                        );
                    }
                    parentBitSet = objectFilters.get(parent);
                    parentNameLength = parent.length() + 1;
                }
                int lastParent = parentBitSet.prevSetBit(currentLevelDoc);
                int offset = 0;
                DocIdSetIterator childIt = childScorer.iterator();
                for (int i = childIt.advance(lastParent + 1); i < currentLevelDoc; i = childIt.nextDoc()) {
                    offset++;
                }
                ni = new SearchHit.NestedIdentity(path.substring(parentNameLength), offset, ni);
                path = parent;
                currentLevelDoc = parentBitSet.nextSetBit(currentLevelDoc);
            }
            return ni;
        }
    }

}
