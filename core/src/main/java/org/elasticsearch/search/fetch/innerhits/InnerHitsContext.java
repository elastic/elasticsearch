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

import org.apache.lucene.index.LeafReader;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.BooleanClause.Occur;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.ConstantScoreScorer;
import org.apache.lucene.search.ConstantScoreWeight;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.search.DocValuesTermsQuery;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.Scorer;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.search.TopDocsCollector;
import org.apache.lucene.search.TopFieldCollector;
import org.apache.lucene.search.TopScoreDocCollector;
import org.apache.lucene.search.Weight;
import org.apache.lucene.search.join.BitSetProducer;
import org.apache.lucene.util.BitSet;
import org.elasticsearch.ExceptionsHelper;
import org.elasticsearch.common.lucene.Lucene;
import org.elasticsearch.common.lucene.search.Queries;
import org.elasticsearch.index.mapper.DocumentMapper;
import org.elasticsearch.index.mapper.MapperService;
import org.elasticsearch.index.mapper.Uid;
import org.elasticsearch.index.mapper.internal.ParentFieldMapper;
import org.elasticsearch.index.mapper.internal.UidFieldMapper;
import org.elasticsearch.index.mapper.object.ObjectMapper;
import org.elasticsearch.search.SearchHitField;
import org.elasticsearch.search.fetch.FetchSubPhase;
import org.elasticsearch.search.internal.InternalSearchHit;
import org.elasticsearch.search.internal.SearchContext;
import org.elasticsearch.search.internal.SubSearchContext;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

/**
 */
public final class InnerHitsContext {

    private final Map<String, BaseInnerHits> innerHits;

    public InnerHitsContext() {
        this.innerHits = new HashMap<>();
    }

    public InnerHitsContext(Map<String, BaseInnerHits> innerHits) {
        this.innerHits = Objects.requireNonNull(innerHits);
    }

    public Map<String, BaseInnerHits> getInnerHits() {
        return innerHits;
    }

    public void addInnerHitDefinition(BaseInnerHits innerHit) {
        if (innerHits.containsKey(innerHit.getName())) {
            throw new IllegalArgumentException("inner_hit definition with the name [" + innerHit.getName() +
                    "] already exists. Use a different inner_hit name");
        }

        innerHits.put(innerHit.getName(), innerHit);
    }

    public static abstract class BaseInnerHits extends SubSearchContext {

        private final String name;
        private InnerHitsContext childInnerHits;

        protected BaseInnerHits(String name, SearchContext context) {
            super(context);
            this.name = name;
        }

        public abstract TopDocs topDocs(SearchContext context, FetchSubPhase.HitContext hitContext) throws IOException;

        public String getName() {
            return name;
        }

        @Override
        public InnerHitsContext innerHits() {
            return childInnerHits;
        }

        public void setChildInnerHits(Map<String, InnerHitsContext.BaseInnerHits> childInnerHits) {
            this.childInnerHits = new InnerHitsContext(childInnerHits);
        }
    }

    public static final class NestedInnerHits extends BaseInnerHits {

        private final ObjectMapper parentObjectMapper;
        private final ObjectMapper childObjectMapper;

        public NestedInnerHits(String name, SearchContext context, ObjectMapper parentObjectMapper, ObjectMapper childObjectMapper) {
            super(name != null ? name : childObjectMapper.fullPath(), context);
            this.parentObjectMapper = parentObjectMapper;
            this.childObjectMapper = childObjectMapper;
        }

        @Override
        public TopDocs topDocs(SearchContext context, FetchSubPhase.HitContext hitContext) throws IOException {
            Query rawParentFilter;
            if (parentObjectMapper == null) {
                rawParentFilter = Queries.newNonNestedFilter();
            } else {
                rawParentFilter = parentObjectMapper.nestedTypeFilter();
            }
            BitSetProducer parentFilter = context.bitsetFilterCache().getBitSetProducer(rawParentFilter);
            Query childFilter = childObjectMapper.nestedTypeFilter();
            Query q = Queries.filtered(query(), new NestedChildrenQuery(parentFilter, childFilter, hitContext));

            if (size() == 0) {
                return new TopDocs(context.searcher().count(q), Lucene.EMPTY_SCORE_DOCS, 0);
            } else {
                int topN = Math.min(from() + size(), context.searcher().getIndexReader().maxDoc());
                TopDocsCollector topDocsCollector;
                if (sort() != null) {
                    try {
                        topDocsCollector = TopFieldCollector.create(sort().sort, topN, true, trackScores(), trackScores());
                    } catch (IOException e) {
                        throw ExceptionsHelper.convertToElastic(e);
                    }
                } else {
                    topDocsCollector = TopScoreDocCollector.create(topN);
                }
                try {
                    context.searcher().search(q, topDocsCollector);
                } finally {
                    clearReleasables(Lifetime.COLLECTION);
                }
                return topDocsCollector.topDocs(from(), size());
            }
        }

        // A filter that only emits the nested children docs of a specific nested parent doc
        static class NestedChildrenQuery extends Query {

            private final BitSetProducer parentFilter;
            private final Query childFilter;
            private final int docId;
            private final LeafReader leafReader;

            NestedChildrenQuery(BitSetProducer parentFilter, Query childFilter, FetchSubPhase.HitContext hitContext) {
                this.parentFilter = parentFilter;
                this.childFilter = childFilter;
                this.docId = hitContext.docId();
                this.leafReader = hitContext.readerContext().reader();
            }

            @Override
            public boolean equals(Object obj) {
                if (sameClassAs(obj) == false) {
                    return false;
                }
                NestedChildrenQuery other = (NestedChildrenQuery) obj;
                return parentFilter.equals(other.parentFilter)
                        && childFilter.equals(other.childFilter)
                        && docId == other.docId
                        && leafReader.getCoreCacheKey() == other.leafReader.getCoreCacheKey();
            }

            @Override
            public int hashCode() {
                int hash = classHash();
                hash = 31 * hash + parentFilter.hashCode();
                hash = 31 * hash + childFilter.hashCode();
                hash = 31 * hash + docId;
                hash = 31 * hash + leafReader.getCoreCacheKey().hashCode();
                return hash;
            }

            @Override
            public String toString(String field) {
                return "NestedChildren(parent=" + parentFilter + ",child=" + childFilter + ")";
            }

            @Override
            public Weight createWeight(IndexSearcher searcher, boolean needsScores) throws IOException {
                final Weight childWeight = childFilter.createWeight(searcher, false);
                return new ConstantScoreWeight(this) {
                    @Override
                    public Scorer scorer(LeafReaderContext context) throws IOException {
                        // Nested docs only reside in a single segment, so no need to evaluate all segments
                        if (!context.reader().getCoreCacheKey().equals(leafReader.getCoreCacheKey())) {
                            return null;
                        }

                        // If docId == 0 then we a parent doc doesn't have child docs, because child docs are stored
                        // before the parent doc and because parent doc is 0 we can safely assume that there are no child docs.
                        if (docId == 0) {
                            return null;
                        }

                        final BitSet parents = parentFilter.getBitSet(context);
                        final int firstChildDocId = parents.prevSetBit(docId - 1) + 1;
                        // A parent doc doesn't have child docs, so we can early exit here:
                        if (firstChildDocId == docId) {
                            return null;
                        }

                        final Scorer childrenScorer = childWeight.scorer(context);
                        if (childrenScorer == null) {
                            return null;
                        }
                        DocIdSetIterator childrenIterator = childrenScorer.iterator();
                        final DocIdSetIterator it = new DocIdSetIterator() {

                            int doc = -1;

                            @Override
                            public int docID() {
                                return doc;
                            }

                            @Override
                            public int nextDoc() throws IOException {
                                return advance(doc + 1);
                            }

                            @Override
                            public int advance(int target) throws IOException {
                                target = Math.max(firstChildDocId, target);
                                if (target >= docId) {
                                    // We're outside the child nested scope, so it is done
                                    return doc = NO_MORE_DOCS;
                                } else {
                                    int advanced = childrenIterator.advance(target);
                                    if (advanced >= docId) {
                                        // We're outside the child nested scope, so it is done
                                        return doc = NO_MORE_DOCS;
                                    } else {
                                        return doc = advanced;
                                    }
                                }
                            }

                            @Override
                            public long cost() {
                                return Math.min(childrenIterator.cost(), docId - firstChildDocId);
                            }

                        };
                        return new ConstantScoreScorer(this, score(), it);
                    }
                };
            }
        }

    }

    public static final class ParentChildInnerHits extends BaseInnerHits {

        private final MapperService mapperService;
        private final DocumentMapper documentMapper;

        public ParentChildInnerHits(String name, SearchContext context, MapperService mapperService, DocumentMapper documentMapper) {
            super(name != null ? name : documentMapper.type(), context);
            this.mapperService = mapperService;
            this.documentMapper = documentMapper;
        }

        @Override
        public TopDocs topDocs(SearchContext context, FetchSubPhase.HitContext hitContext) throws IOException {
            final Query hitQuery;
            if (isParentHit(hitContext.hit())) {
                String field = ParentFieldMapper.joinField(hitContext.hit().type());
                hitQuery = new DocValuesTermsQuery(field, hitContext.hit().id());
            } else if (isChildHit(hitContext.hit())) {
                DocumentMapper hitDocumentMapper = mapperService.documentMapper(hitContext.hit().type());
                final String parentType = hitDocumentMapper.parentFieldMapper().type();
                SearchHitField parentField = hitContext.hit().field(ParentFieldMapper.NAME);
                if (parentField == null) {
                    throw new IllegalStateException("All children must have a _parent");
                }
                hitQuery = new TermQuery(new Term(UidFieldMapper.NAME, Uid.createUid(parentType, parentField.getValue())));
            } else {
                return Lucene.EMPTY_TOP_DOCS;
            }

            BooleanQuery q = new BooleanQuery.Builder()
                .add(query(), Occur.MUST)
                // Only include docs that have the current hit as parent
                .add(hitQuery, Occur.FILTER)
                // Only include docs that have this inner hits type
                .add(documentMapper.typeFilter(), Occur.FILTER)
                .build();
            if (size() == 0) {
                final int count = context.searcher().count(q);
                return new TopDocs(count, Lucene.EMPTY_SCORE_DOCS, 0);
            } else {
                int topN = Math.min(from() + size(), context.searcher().getIndexReader().maxDoc());
                TopDocsCollector topDocsCollector;
                if (sort() != null) {
                    topDocsCollector = TopFieldCollector.create(sort().sort, topN, true, trackScores(), trackScores());
                } else {
                    topDocsCollector = TopScoreDocCollector.create(topN);
                }
                try {
                    context.searcher().search(q, topDocsCollector);
                } finally {
                    clearReleasables(Lifetime.COLLECTION);
                }
                return topDocsCollector.topDocs(from(), size());
            }
        }

        private boolean isParentHit(InternalSearchHit hit) {
            return hit.type().equals(documentMapper.parentFieldMapper().type());
        }

        private boolean isChildHit(InternalSearchHit hit) {
            DocumentMapper hitDocumentMapper = mapperService.documentMapper(hit.type());
            return documentMapper.type().equals(hitDocumentMapper.parentFieldMapper().type());
        }
    }
}
