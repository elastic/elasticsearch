/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
package org.elasticsearch.join.query;

import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.ReaderUtil;
import org.apache.lucene.index.SortedDocValues;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.BooleanClause;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.MultiCollector;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.ScoreMode;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.search.TopDocsCollector;
import org.apache.lucene.search.TopFieldCollector;
import org.apache.lucene.search.TopScoreDocCollector;
import org.apache.lucene.search.TotalHitCountCollector;
import org.apache.lucene.search.TotalHits;
import org.apache.lucene.search.Weight;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.ExceptionsHelper;
import org.elasticsearch.action.search.MaxScoreCollector;
import org.elasticsearch.common.lucene.Lucene;
import org.elasticsearch.common.lucene.search.TopDocsAndMaxScore;
import org.elasticsearch.index.mapper.IdFieldMapper;
import org.elasticsearch.index.query.InnerHitBuilder;
import org.elasticsearch.index.query.InnerHitContextBuilder;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.SearchExecutionContext;
import org.elasticsearch.join.mapper.Joiner;
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

    ParentChildInnerHitContextBuilder(
        String typeName,
        boolean fetchChildInnerHits,
        QueryBuilder query,
        InnerHitBuilder innerHitBuilder,
        Map<String, InnerHitContextBuilder> children
    ) {
        super(query, innerHitBuilder, children);
        this.typeName = typeName;
        this.fetchChildInnerHits = fetchChildInnerHits;
    }

    @Override
    protected void doBuild(SearchContext context, InnerHitsContext innerHitsContext) throws IOException {
        SearchExecutionContext searchExecutionContext = context.getSearchExecutionContext();
        Joiner joiner = Joiner.getJoiner(searchExecutionContext);
        if (joiner != null) {
            String name = innerHitBuilder.getName() != null ? innerHitBuilder.getName() : typeName;
            JoinFieldInnerHitSubContext joinFieldInnerHits = new JoinFieldInnerHitSubContext(
                name,
                context,
                typeName,
                fetchChildInnerHits,
                joiner
            );
            setupInnerHitsContext(searchExecutionContext, joinFieldInnerHits);
            innerHitsContext.addInnerHitDefinition(joinFieldInnerHits);
        } else {
            if (innerHitBuilder.isIgnoreUnmapped() == false) {
                throw new IllegalStateException("no join field has been configured");
            }
        }
    }

    static final class JoinFieldInnerHitSubContext extends InnerHitsContext.InnerHitSubContext {
        private final String typeName;
        private final boolean fetchChildInnerHits;
        private final Joiner joiner;

        JoinFieldInnerHitSubContext(String name, SearchContext context, String typeName, boolean fetchChildInnerHits, Joiner joiner) {
            super(name, context);
            this.typeName = typeName;
            this.fetchChildInnerHits = fetchChildInnerHits;
            this.joiner = joiner;
        }

        @Override
        public TopDocsAndMaxScore topDocs(SearchHit hit) throws IOException {
            Weight innerHitQueryWeight = getInnerHitQueryWeight();
            String joinName = getSortedDocValue(joiner.getJoinField(), context, hit.docId());
            if (joinName == null) {
                return new TopDocsAndMaxScore(Lucene.EMPTY_TOP_DOCS, Float.NaN);
            }

            SearchExecutionContext context = this.context.getSearchExecutionContext();
            Query q;
            if (fetchChildInnerHits) {
                Query hitQuery = new TermQuery(new Term(joiner.parentJoinField(typeName), hit.getId()));
                q = new BooleanQuery.Builder()
                    // Only include child documents that have the current hit as parent:
                    .add(hitQuery, BooleanClause.Occur.FILTER)
                    // and only include child documents of a single relation:
                    .add(new TermQuery(new Term(joiner.getJoinField(), typeName)), BooleanClause.Occur.FILTER)
                    .build();
            } else {
                String parentId = getSortedDocValue(joiner.childJoinField(typeName), this.context, hit.docId());
                if (parentId == null) {
                    return new TopDocsAndMaxScore(Lucene.EMPTY_TOP_DOCS, Float.NaN);
                }
                q = this.context.getSearchExecutionContext().getFieldType(IdFieldMapper.NAME).termQuery(parentId, context);
            }

            Weight weight = this.context.searcher().createWeight(this.context.searcher().rewrite(q), ScoreMode.COMPLETE_NO_SCORES, 1f);
            if (size() == 0) {
                TotalHitCountCollector totalHitCountCollector = new TotalHitCountCollector();
                for (LeafReaderContext ctx : this.context.searcher().getIndexReader().leaves()) {
                    intersect(weight, innerHitQueryWeight, totalHitCountCollector, ctx);
                }
                return new TopDocsAndMaxScore(
                    new TopDocs(new TotalHits(totalHitCountCollector.getTotalHits(), TotalHits.Relation.EQUAL_TO), Lucene.EMPTY_SCORE_DOCS),
                    Float.NaN
                );
            } else {
                int topN = Math.min(from() + size(), this.context.searcher().getIndexReader().maxDoc());
                TopDocsCollector<?> topDocsCollector;
                MaxScoreCollector maxScoreCollector = null;
                if (sort() != null) {
                    topDocsCollector = TopFieldCollector.create(sort().sort, topN, Integer.MAX_VALUE);
                    if (trackScores()) {
                        maxScoreCollector = new MaxScoreCollector();
                    }
                } else {
                    topDocsCollector = TopScoreDocCollector.create(topN, Integer.MAX_VALUE);
                    maxScoreCollector = new MaxScoreCollector();
                }
                for (LeafReaderContext ctx : this.context.searcher().getIndexReader().leaves()) {
                    intersect(weight, innerHitQueryWeight, MultiCollector.wrap(topDocsCollector, maxScoreCollector), ctx);
                }
                TopDocs topDocs = topDocsCollector.topDocs(from(), size());
                float maxScore = Float.NaN;
                if (maxScoreCollector != null) {
                    maxScore = maxScoreCollector.getMaxScore();
                }
                return new TopDocsAndMaxScore(topDocs, maxScore);
            }
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

}
