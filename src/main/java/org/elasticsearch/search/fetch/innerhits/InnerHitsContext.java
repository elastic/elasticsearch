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

package org.elasticsearch.search.fetch.innerhits;

import com.google.common.collect.ImmutableMap;

import org.apache.lucene.index.LeafReader;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.BooleanClause.Occur;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.DocIdSet;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.search.Filter;
import org.apache.lucene.search.FilteredQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.search.TopDocsCollector;
import org.apache.lucene.search.TopFieldCollector;
import org.apache.lucene.search.TopScoreDocCollector;
import org.apache.lucene.search.join.BitDocIdSetFilter;
import org.apache.lucene.util.BitSet;
import org.apache.lucene.util.Bits;
import org.elasticsearch.ExceptionsHelper;
import org.elasticsearch.common.lucene.Lucene;
import org.elasticsearch.common.lucene.search.Queries;
import org.elasticsearch.index.fieldvisitor.SingleFieldsVisitor;
import org.elasticsearch.index.mapper.DocumentMapper;
import org.elasticsearch.index.mapper.Uid;
import org.elasticsearch.index.mapper.internal.ParentFieldMapper;
import org.elasticsearch.index.mapper.internal.UidFieldMapper;
import org.elasticsearch.index.mapper.object.ObjectMapper;
import org.elasticsearch.index.query.ParsedQuery;
import org.elasticsearch.search.SearchHitField;
import org.elasticsearch.search.fetch.FetchSubPhase;
import org.elasticsearch.search.internal.FilteredSearchContext;
import org.elasticsearch.search.internal.SearchContext;

import java.io.IOException;
import java.util.Map;

/**
 */
public final class InnerHitsContext {

    private final Map<String, BaseInnerHits> innerHits;

    public InnerHitsContext(Map<String, BaseInnerHits> innerHits) {
        this.innerHits = innerHits;
    }

    public Map<String, BaseInnerHits> getInnerHits() {
        return innerHits;
    }

    public void addInnerHitDefinition(String name, BaseInnerHits innerHit) {
        innerHits.put(name, innerHit);
    }

    public static abstract class BaseInnerHits extends FilteredSearchContext {

        protected final Query query;
        private final InnerHitsContext childInnerHits;

        protected BaseInnerHits(SearchContext context, Query query, Map<String, BaseInnerHits> childInnerHits) {
            super(context);
            this.query = query;
            if (childInnerHits != null && !childInnerHits.isEmpty()) {
                this.childInnerHits = new InnerHitsContext(childInnerHits);
            } else {
                this.childInnerHits = null;
            }
        }

        @Override
        public Query query() {
            return query;
        }

        @Override
        public ParsedQuery parsedQuery() {
            return new ParsedQuery(query, ImmutableMap.<String, Filter>of());
        }

        public abstract TopDocs topDocs(SearchContext context, FetchSubPhase.HitContext hitContext) throws IOException;

        @Override
        public InnerHitsContext innerHits() {
            return childInnerHits;
        }

    }

    public static final class NestedInnerHits extends BaseInnerHits {

        private final ObjectMapper parentObjectMapper;
        private final ObjectMapper childObjectMapper;

        public NestedInnerHits(SearchContext context, Query query, Map<String, BaseInnerHits> childInnerHits, ObjectMapper parentObjectMapper, ObjectMapper childObjectMapper) {
            super(context, query, childInnerHits);
            this.parentObjectMapper = parentObjectMapper;
            this.childObjectMapper = childObjectMapper;
        }

        @Override
        public TopDocs topDocs(SearchContext context, FetchSubPhase.HitContext hitContext) throws IOException {
            Filter rawParentFilter;
            if (parentObjectMapper == null) {
                rawParentFilter = Queries.newNonNestedFilter();
            } else {
                rawParentFilter = parentObjectMapper.nestedTypeFilter();
            }
            BitDocIdSetFilter parentFilter = context.bitsetFilterCache().getBitDocIdSetFilter(rawParentFilter);
            Filter childFilter = context.filterCache().cache(childObjectMapper.nestedTypeFilter(), null, context.queryParserService().autoFilterCachePolicy());
            Query q = new FilteredQuery(query, new NestedChildrenFilter(parentFilter, childFilter, hitContext));

            if (size() == 0) {
                return new TopDocs(context.searcher().count(q), Lucene.EMPTY_SCORE_DOCS, 0);
            } else {
                int topN = from() + size();
                TopDocsCollector topDocsCollector;
                if (sort() != null) {
                    try {
                        topDocsCollector = TopFieldCollector.create(sort(), topN, true, trackScores(), trackScores());
                    } catch (IOException e) {
                        throw ExceptionsHelper.convertToElastic(e);
                    }
                } else {
                    topDocsCollector = TopScoreDocCollector.create(topN);
                }
                context.searcher().search(q, topDocsCollector);
                return topDocsCollector.topDocs(from(), size());
            }
        }

        // A filter that only emits the nested children docs of a specific nested parent doc
        static class NestedChildrenFilter extends Filter {

            private final BitDocIdSetFilter parentFilter;
            private final Filter childFilter;
            private final int docId;
            private final LeafReader atomicReader;

            NestedChildrenFilter(BitDocIdSetFilter parentFilter, Filter childFilter, FetchSubPhase.HitContext hitContext) {
                this.parentFilter = parentFilter;
                this.childFilter = childFilter;
                this.docId = hitContext.docId();
                this.atomicReader = hitContext.readerContext().reader();
            }

            @Override
            public String toString(String field) {
                return "NestedChildren(parent=" + parentFilter + ",child=" + childFilter + ")";
            }

            @Override
            public DocIdSet getDocIdSet(LeafReaderContext context, final Bits acceptDocs) throws IOException {
                // Nested docs only reside in a single segment, so no need to evaluate all segments
                if (!context.reader().getCoreCacheKey().equals(this.atomicReader.getCoreCacheKey())) {
                    return null;
                }

                // If docId == 0 then we a parent doc doesn't have child docs, because child docs are stored
                // before the parent doc and because parent doc is 0 we can safely assume that there are no child docs.
                if (docId == 0) {
                    return null;
                }

                final BitSet parents = parentFilter.getDocIdSet(context).bits();
                final int firstChildDocId = parents.prevSetBit(docId - 1) + 1;
                // A parent doc doesn't have child docs, so we can early exit here:
                if (firstChildDocId == docId) {
                    return null;
                }

                final DocIdSet children = childFilter.getDocIdSet(context, acceptDocs);
                if (children == null) {
                    return null;
                }
                final DocIdSetIterator childrenIterator = children.iterator();
                if (childrenIterator == null) {
                    return null;
                }
                return new DocIdSet() {

                    @Override
                    public long ramBytesUsed() {
                        return parents.ramBytesUsed() + children.ramBytesUsed();
                    }

                    @Override
                    public DocIdSetIterator iterator() throws IOException {
                        return new DocIdSetIterator() {

                            int currentDocId = -1;

                            @Override
                            public int docID() {
                                return currentDocId;
                            }

                            @Override
                            public int nextDoc() throws IOException {
                                return advance(currentDocId + 1);
                            }

                            @Override
                            public int advance(int target) throws IOException {
                                target = Math.max(firstChildDocId, target);
                                if (target >= docId) {
                                    // We're outside the child nested scope, so it is done
                                    return currentDocId = NO_MORE_DOCS;
                                } else {
                                    int advanced = childrenIterator.advance(target);
                                    if (advanced >= docId) {
                                        // We're outside the child nested scope, so it is done
                                        return currentDocId = NO_MORE_DOCS;
                                    } else {
                                        return currentDocId = advanced;
                                    }
                                }
                            }

                            @Override
                            public long cost() {
                                return childrenIterator.cost();
                            }
                        };
                    }
                };
            }
        }

    }

    public static final class ParentChildInnerHits extends BaseInnerHits {

        private final DocumentMapper documentMapper;

        public ParentChildInnerHits(SearchContext context, Query query, Map<String, BaseInnerHits> childInnerHits, DocumentMapper documentMapper) {
            super(context, query, childInnerHits);
            this.documentMapper = documentMapper;
        }

        @Override
        public TopDocs topDocs(SearchContext context, FetchSubPhase.HitContext hitContext) throws IOException {
            final String term;
            final String field;
            if (documentMapper.parentFieldMapper().active()) {
                // Active _parent field has been selected, so we want a children doc as inner hits.
                field = ParentFieldMapper.NAME;
                term = Uid.createUid(hitContext.hit().type(), hitContext.hit().id());
            } else {
                // No active _parent field has been selected, so we want parent docs as inner hits.
                field = UidFieldMapper.NAME;
                SearchHitField parentField = hitContext.hit().field(ParentFieldMapper.NAME);
                if (parentField != null) {
                    term = parentField.getValue();
                } else {
                    SingleFieldsVisitor fieldsVisitor = new SingleFieldsVisitor(ParentFieldMapper.NAME);
                    hitContext.reader().document(hitContext.docId(), fieldsVisitor);
                    if (fieldsVisitor.fields().isEmpty()) {
                        return Lucene.EMPTY_TOP_DOCS;
                    }
                    term = (String) fieldsVisitor.fields().get(ParentFieldMapper.NAME).get(0);
                }
            }
            Filter filter = Queries.wrap(new TermQuery(new Term(field, term))); // Only include docs that have the current hit as parent
            Filter typeFilter = documentMapper.typeFilter(); // Only include docs that have this inner hits type.

            BooleanQuery filteredQuery = new BooleanQuery();
            filteredQuery.add(query, Occur.MUST);
            filteredQuery.add(filter, Occur.FILTER);
            filteredQuery.add(typeFilter, Occur.FILTER);
            if (size() == 0) {
                final int count = context.searcher().count(filteredQuery);
                return new TopDocs(count, Lucene.EMPTY_SCORE_DOCS, 0);
            } else {
                int topN = from() + size();
                TopDocsCollector topDocsCollector;
                if (sort() != null) {
                    topDocsCollector = TopFieldCollector.create(sort(), topN, true, trackScores(), trackScores());
                } else {
                    topDocsCollector = TopScoreDocCollector.create(topN);
                }
                context.searcher().search( filteredQuery, topDocsCollector);
                return topDocsCollector.topDocs(from(), size());
            }
        }
    }
}
