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
package org.elasticsearch.join.query;

import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.ReaderUtil;
import org.apache.lucene.index.SortedDocValues;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.BooleanClause;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.DocValuesTermsQuery;
import org.apache.lucene.search.MatchNoDocsQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.search.TopDocsCollector;
import org.apache.lucene.search.TopFieldCollector;
import org.apache.lucene.search.TopScoreDocCollector;
import org.apache.lucene.search.TotalHitCountCollector;
import org.apache.lucene.search.Weight;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.ExceptionsHelper;
import org.elasticsearch.common.document.DocumentField;
import org.elasticsearch.common.lucene.Lucene;
import org.elasticsearch.index.mapper.DocumentMapper;
import org.elasticsearch.index.mapper.IdFieldMapper;
import org.elasticsearch.index.mapper.MapperService;
import org.elasticsearch.index.mapper.ParentFieldMapper;
import org.elasticsearch.index.query.InnerHitBuilder;
import org.elasticsearch.index.query.InnerHitContextBuilder;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryShardContext;
import org.elasticsearch.join.mapper.ParentIdFieldMapper;
import org.elasticsearch.join.mapper.ParentJoinFieldMapper;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.fetch.subphase.InnerHitsContext;
import org.elasticsearch.search.internal.SearchContext;

import java.io.IOException;
import java.util.List;
import java.util.Map;

import static org.elasticsearch.search.fetch.subphase.InnerHitsContext.intersect;

class ParentChildInnerHitContextBuilder extends InnerHitContextBuilder {
    private final String typeName;
    private final boolean fetchChildInnerHits;

    ParentChildInnerHitContextBuilder(String typeName, boolean fetchChildInnerHits, QueryBuilder query,
                                      InnerHitBuilder innerHitBuilder, Map<String, InnerHitContextBuilder> children) {
        super(query, innerHitBuilder, children);
        this.typeName = typeName;
        this.fetchChildInnerHits = fetchChildInnerHits;
    }

    @Override
    protected void doBuild(SearchContext parentSearchContext, InnerHitsContext innerHitsContext) throws IOException {
        if (parentSearchContext.mapperService().getIndexSettings().isSingleType()) {
            handleJoinFieldInnerHits(parentSearchContext, innerHitsContext);
        } else {
            handleParentFieldInnerHits(parentSearchContext, innerHitsContext);
        }
    }

    private void handleJoinFieldInnerHits(SearchContext context, InnerHitsContext innerHitsContext) throws IOException {
        QueryShardContext queryShardContext = context.getQueryShardContext();
        ParentJoinFieldMapper joinFieldMapper = ParentJoinFieldMapper.getMapper(context.mapperService());
        if (joinFieldMapper != null) {
            String name = innerHitBuilder.getName() != null ? innerHitBuilder.getName() : typeName;
            JoinFieldInnerHitSubContext joinFieldInnerHits = new JoinFieldInnerHitSubContext(name, context, typeName,
                fetchChildInnerHits, joinFieldMapper);
            setupInnerHitsContext(queryShardContext, joinFieldInnerHits);
            innerHitsContext.addInnerHitDefinition(joinFieldInnerHits);
        } else {
            if (innerHitBuilder.isIgnoreUnmapped() == false) {
                throw new IllegalStateException("no join field has been configured");
            }
        }
    }

    private void handleParentFieldInnerHits(SearchContext context, InnerHitsContext innerHitsContext) throws IOException {
        QueryShardContext queryShardContext = context.getQueryShardContext();
        DocumentMapper documentMapper = queryShardContext.documentMapper(typeName);
        if (documentMapper == null) {
            if (innerHitBuilder.isIgnoreUnmapped() == false) {
                throw new IllegalStateException("[" + query.getName() + "] no mapping found for type [" + typeName + "]");
            } else {
                return;
            }
        }
        String name = innerHitBuilder.getName() != null ? innerHitBuilder.getName() : documentMapper.type();
        ParentChildInnerHitSubContext parentChildInnerHits = new ParentChildInnerHitSubContext(
            name, context, queryShardContext.getMapperService(), documentMapper
        );
        setupInnerHitsContext(queryShardContext, parentChildInnerHits);
        innerHitsContext.addInnerHitDefinition(parentChildInnerHits);
    }

    static final class JoinFieldInnerHitSubContext extends InnerHitsContext.InnerHitSubContext {
        private final String typeName;
        private final boolean fetchChildInnerHits;
        private final ParentJoinFieldMapper joinFieldMapper;

        JoinFieldInnerHitSubContext(String name, SearchContext context, String typeName, boolean fetchChildInnerHits,
                                    ParentJoinFieldMapper joinFieldMapper) {
            super(name, context);
            this.typeName = typeName;
            this.fetchChildInnerHits = fetchChildInnerHits;
            this.joinFieldMapper = joinFieldMapper;
        }

        @Override
        public TopDocs[] topDocs(SearchHit[] hits) throws IOException {
            Weight innerHitQueryWeight = createInnerHitQueryWeight();
            TopDocs[] result = new TopDocs[hits.length];
            for (int i = 0; i < hits.length; i++) {
                SearchHit hit = hits[i];
                String joinName = getSortedDocValue(joinFieldMapper.name(), context, hit.docId());
                if (joinName == null) {
                    result[i] = Lucene.EMPTY_TOP_DOCS;
                    continue;
                }

                QueryShardContext qsc = context.getQueryShardContext();
                ParentIdFieldMapper parentIdFieldMapper =
                    joinFieldMapper.getParentIdFieldMapper(typeName, fetchChildInnerHits == false);
                if (parentIdFieldMapper == null) {
                    result[i] = Lucene.EMPTY_TOP_DOCS;
                    continue;
                }

                Query q;
                if (fetchChildInnerHits) {
                    Query hitQuery = parentIdFieldMapper.fieldType().termQuery(hit.getId(), qsc);
                    q = new BooleanQuery.Builder()
                        // Only include child documents that have the current hit as parent:
                        .add(hitQuery, BooleanClause.Occur.FILTER)
                        // and only include child documents of a single relation:
                        .add(joinFieldMapper.fieldType().termQuery(typeName, qsc), BooleanClause.Occur.FILTER)
                        .build();
                } else {
                    String parentId = getSortedDocValue(parentIdFieldMapper.name(), context, hit.docId());
                    q = context.mapperService().fullName(IdFieldMapper.NAME).termQuery(parentId, qsc);
                }

                Weight weight = context.searcher().createNormalizedWeight(q, false);
                if (size() == 0) {
                    TotalHitCountCollector totalHitCountCollector = new TotalHitCountCollector();
                    for (LeafReaderContext ctx : context.searcher().getIndexReader().leaves()) {
                        intersect(weight, innerHitQueryWeight, totalHitCountCollector, ctx);
                    }
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

        private String getSortedDocValue(String field, SearchContext context, int docId) {
            try {
                List<LeafReaderContext> ctxs = context.searcher().getIndexReader().leaves();
                LeafReaderContext ctx = ctxs.get(ReaderUtil.subIndex(docId, ctxs));
                SortedDocValues docValues = ctx.reader().getSortedDocValues(field);
                int segmentDocId = docId - ctx.docBase;
                if (docValues == null || docValues.advanceExact(segmentDocId) == false) {
                    return null;
                }
                int ord = docValues.ordValue();
                BytesRef joinName = docValues.lookupOrd(ord);
                return joinName.utf8ToString();
            } catch (IOException e) {
                throw ExceptionsHelper.convertToElastic(e);
            }
        }

    }

    static final class ParentChildInnerHitSubContext extends InnerHitsContext.InnerHitSubContext {
        private final MapperService mapperService;
        private final DocumentMapper documentMapper;

        ParentChildInnerHitSubContext(String name, SearchContext context, MapperService mapperService, DocumentMapper documentMapper) {
            super(name, context);
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
                    DocumentField parentField = hit.field(ParentFieldMapper.NAME);
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
                    .add(hitQuery, BooleanClause.Occur.FILTER)
                    // Only include docs that have this inner hits type
                    .add(documentMapper.typeFilter(context.getQueryShardContext()), BooleanClause.Occur.FILTER)
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
                    TopDocsCollector<?> topDocsCollector;
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
}
