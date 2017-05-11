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

package org.elasticsearch.search.fetch.subphase;

import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.ReaderUtil;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.BooleanClause.Occur;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.CollectionTerminatedException;
import org.apache.lucene.search.Collector;
import org.apache.lucene.search.ConjunctionDISI;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.search.DocValuesTermsQuery;
import org.apache.lucene.search.LeafCollector;
import org.apache.lucene.search.MatchNoDocsQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.Scorer;
import org.apache.lucene.search.ScorerSupplier;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.search.TopDocsCollector;
import org.apache.lucene.search.TopFieldCollector;
import org.apache.lucene.search.TopScoreDocCollector;
import org.apache.lucene.search.TotalHitCountCollector;
import org.apache.lucene.search.Weight;
import org.apache.lucene.search.join.BitSetProducer;
import org.apache.lucene.search.join.ParentChildrenBlockJoinQuery;
import org.apache.lucene.util.Bits;
import org.elasticsearch.common.lucene.Lucene;
import org.elasticsearch.common.lucene.search.Queries;
import org.elasticsearch.index.mapper.DocumentMapper;
import org.elasticsearch.index.mapper.MapperService;
import org.elasticsearch.index.mapper.ObjectMapper;
import org.elasticsearch.index.mapper.ParentFieldMapper;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHitField;
import org.elasticsearch.search.internal.SearchContext;
import org.elasticsearch.search.internal.SubSearchContext;

import java.io.IOException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

public final class InnerHitsContext {

    private final Map<String, BaseInnerHits> innerHits;

    public InnerHitsContext() {
        this.innerHits = new HashMap<>();
    }

    InnerHitsContext(Map<String, BaseInnerHits> innerHits) {
        this.innerHits = Objects.requireNonNull(innerHits);
    }

    public Map<String, BaseInnerHits> getInnerHits() {
        return innerHits;
    }

    public void addInnerHitDefinition(BaseInnerHits innerHit) {
        if (innerHits.containsKey(innerHit.getName())) {
            throw new IllegalArgumentException("inner_hit definition with the name [" + innerHit.getName() +
                    "] already exists. Use a different inner_hit name or define one explicitly");
        }

        innerHits.put(innerHit.getName(), innerHit);
    }

    public abstract static class BaseInnerHits extends SubSearchContext {

        private final String name;
        final SearchContext context;
        private InnerHitsContext childInnerHits;

        BaseInnerHits(String name, SearchContext context) {
            super(context);
            this.name = name;
            this.context = context;
        }

        public abstract TopDocs[] topDocs(SearchHit[] hits) throws IOException;

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

        Weight createInnerHitQueryWeight() throws IOException {
            final boolean needsScores = size() != 0 && (sort() == null || sort().sort.needsScores());
            return context.searcher().createNormalizedWeight(query(), needsScores);
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
        public TopDocs[] topDocs(SearchHit[] hits) throws IOException {
            Weight innerHitQueryWeight = createInnerHitQueryWeight();
            TopDocs[] result = new TopDocs[hits.length];
            for (int i = 0; i < hits.length; i++) {
                SearchHit hit = hits[i];
                Query rawParentFilter;
                if (parentObjectMapper == null) {
                    rawParentFilter = Queries.newNonNestedFilter();
                } else {
                    rawParentFilter = parentObjectMapper.nestedTypeFilter();
                }

                int parentDocId = hit.docId();
                final int readerIndex = ReaderUtil.subIndex(parentDocId, searcher().getIndexReader().leaves());
                // With nested inner hits the nested docs are always in the same segement, so need to use the other segments
                LeafReaderContext ctx = searcher().getIndexReader().leaves().get(readerIndex);

                Query childFilter = childObjectMapper.nestedTypeFilter();
                BitSetProducer parentFilter = context.bitsetFilterCache().getBitSetProducer(rawParentFilter);
                Query q = new ParentChildrenBlockJoinQuery(parentFilter, childFilter, parentDocId);
                Weight weight = context.searcher().createNormalizedWeight(q, false);
                if (size() == 0) {
                    TotalHitCountCollector totalHitCountCollector = new TotalHitCountCollector();
                    intersect(weight, innerHitQueryWeight, totalHitCountCollector, ctx);
                    result[i] = new TopDocs(totalHitCountCollector.getTotalHits(), Lucene.EMPTY_SCORE_DOCS, 0);
                } else {
                    int topN = Math.min(from() + size(), context.searcher().getIndexReader().maxDoc());
                    TopDocsCollector<?> topDocsCollector;
                    if (sort() != null) {
                        topDocsCollector = TopFieldCollector.create(sort().sort, topN, true, trackScores(), trackScores());
                    } else {
                        topDocsCollector = TopScoreDocCollector.create(topN);
                    }
                    try {
                        intersect(weight, innerHitQueryWeight, topDocsCollector, ctx);
                    } finally {
                        clearReleasables(Lifetime.COLLECTION);
                    }
                    result[i] = topDocsCollector.topDocs(from(), size());
                }
            }
            return result;
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
        public TopDocs[] topDocs(SearchHit[] hits) throws IOException {
            Weight innerHitQueryWeight = createInnerHitQueryWeight();
            TopDocs[] result = new TopDocs[hits.length];
            for (int i = 0; i < hits.length; i++) {
                SearchHit hit = hits[i];
                final Query hitQuery;
                if (isParentHit(hit)) {
                    String field = ParentFieldMapper.joinField(hit.getType());
                    hitQuery = new DocValuesTermsQuery(field, hit.getId());
                } else if (isChildHit(hit)) {
                    DocumentMapper hitDocumentMapper = mapperService.documentMapper(hit.getType());
                    final String parentType = hitDocumentMapper.parentFieldMapper().type();
                    SearchHitField parentField = hit.field(ParentFieldMapper.NAME);
                    if (parentField == null) {
                        throw new IllegalStateException("All children must have a _parent");
                    }
                    Term uidTerm = context.mapperService().createUidTerm(parentType, parentField.getValue());
                    if (uidTerm == null) {
                        hitQuery = new MatchNoDocsQuery("Missing type: " + parentType);
                    } else {
                        hitQuery = new TermQuery(uidTerm);
                    }
                } else {
                    result[i] = Lucene.EMPTY_TOP_DOCS;
                    continue;
                }

                BooleanQuery q = new BooleanQuery.Builder()
                    // Only include docs that have the current hit as parent
                    .add(hitQuery, Occur.FILTER)
                    // Only include docs that have this inner hits type
                    .add(documentMapper.typeFilter(context.getQueryShardContext()), Occur.FILTER)
                    .build();
                Weight weight = context.searcher().createNormalizedWeight(q, false);
                if (size() == 0) {
                    TotalHitCountCollector totalHitCountCollector = new TotalHitCountCollector();
                    for (LeafReaderContext ctx : context.searcher().getIndexReader().leaves()) {
                        intersect(weight, innerHitQueryWeight, totalHitCountCollector, ctx);
                    }
                    result[i] = new TopDocs(totalHitCountCollector.getTotalHits(), Lucene.EMPTY_SCORE_DOCS, 0);
                } else {
                    int topN = Math.min(from() + size(), context.searcher().getIndexReader().maxDoc());
                    TopDocsCollector topDocsCollector;
                    if (sort() != null) {
                        topDocsCollector = TopFieldCollector.create(sort().sort, topN, true, trackScores(), trackScores());
                    } else {
                        topDocsCollector = TopScoreDocCollector.create(topN);
                    }
                    try {
                        for (LeafReaderContext ctx : context.searcher().getIndexReader().leaves()) {
                            intersect(weight, innerHitQueryWeight, topDocsCollector, ctx);
                        }
                    } finally {
                        clearReleasables(Lifetime.COLLECTION);
                    }
                    result[i] = topDocsCollector.topDocs(from(), size());
                }
            }
            return result;
        }

        private boolean isParentHit(SearchHit hit) {
            return hit.getType().equals(documentMapper.parentFieldMapper().type());
        }

        private boolean isChildHit(SearchHit hit) {
            DocumentMapper hitDocumentMapper = mapperService.documentMapper(hit.getType());
            return documentMapper.type().equals(hitDocumentMapper.parentFieldMapper().type());
        }
    }

    static void intersect(Weight weight, Weight innerHitQueryWeight, Collector collector, LeafReaderContext ctx) throws IOException {
        ScorerSupplier scorerSupplier = weight.scorerSupplier(ctx);
        if (scorerSupplier == null) {
            return;
        }
        // use random access since this scorer will be consumed on a minority of documents
        Scorer scorer = scorerSupplier.get(true);

        ScorerSupplier innerHitQueryScorerSupplier = innerHitQueryWeight.scorerSupplier(ctx);
        if (innerHitQueryScorerSupplier == null) {
            return;
        }
        // use random access since this scorer will be consumed on a minority of documents
        Scorer innerHitQueryScorer = innerHitQueryScorerSupplier.get(true);

        final LeafCollector leafCollector;
        try {
            leafCollector = collector.getLeafCollector(ctx);
            // Just setting the innerHitQueryScorer is ok, because that is the actual scoring part of the query
            leafCollector.setScorer(innerHitQueryScorer);
        } catch (CollectionTerminatedException e) {
            return;
        }

        try {
            Bits acceptDocs = ctx.reader().getLiveDocs();
            DocIdSetIterator iterator = ConjunctionDISI.intersectIterators(Arrays.asList(innerHitQueryScorer.iterator(),
                scorer.iterator()));
            for (int docId = iterator.nextDoc(); docId < DocIdSetIterator.NO_MORE_DOCS; docId = iterator.nextDoc()) {
                if (acceptDocs == null || acceptDocs.get(docId)) {
                    leafCollector.collect(docId);
                }
            }
        } catch (CollectionTerminatedException e) {
            // ignore and continue
        }
    }
}
