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
package org.elasticsearch.percolator;

import com.carrotsearch.hppc.FloatArrayList;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.search.BooleanClause.Occur;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.LeafCollector;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.Scorer;
import org.apache.lucene.search.SimpleCollector;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.search.TopScoreDocCollector;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.common.logging.ESLogger;
import org.elasticsearch.common.lucene.Lucene;
import org.elasticsearch.common.lucene.search.Queries;
import org.elasticsearch.index.fielddata.IndexFieldData;
import org.elasticsearch.index.fielddata.SortedBinaryDocValues;
import org.elasticsearch.index.mapper.MappedFieldType;
import org.elasticsearch.index.mapper.Uid;
import org.elasticsearch.index.mapper.internal.UidFieldMapper;
import org.elasticsearch.index.query.ParsedQuery;
import org.elasticsearch.search.aggregations.Aggregator;
import org.elasticsearch.search.aggregations.BucketCollector;
import org.elasticsearch.search.aggregations.bucket.global.GlobalAggregator;
import org.elasticsearch.search.aggregations.support.AggregationContext;
import org.elasticsearch.search.highlight.HighlightField;
import org.elasticsearch.search.highlight.HighlightPhase;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentMap;

/**
 */
abstract class QueryCollector extends SimpleCollector {

    final IndexFieldData<?> uidFieldData;
    final IndexSearcher searcher;
    final ConcurrentMap<BytesRef, Query> queries;
    final ESLogger logger;
    boolean isNestedDoc = false;

    BytesRef current;

    SortedBinaryDocValues values;

    final BucketCollector aggregatorCollector;
    LeafCollector aggregatorLeafCollector;

    QueryCollector(ESLogger logger, PercolateContext context, boolean isNestedDoc) throws IOException {
        this.logger = logger;
        this.queries = context.percolateQueries();
        this.searcher = context.docSearcher();
        final MappedFieldType uidMapper = context.mapperService().fullName(UidFieldMapper.NAME);
        this.uidFieldData = context.fieldData().getForField(uidMapper);
        this.isNestedDoc = isNestedDoc;

        List<Aggregator> aggregatorCollectors = new ArrayList<>();

        if (context.aggregations() != null) {
            AggregationContext aggregationContext = new AggregationContext(context);
            context.aggregations().aggregationContext(aggregationContext);

            Aggregator[] aggregators = context.aggregations().factories().createTopLevelAggregators(aggregationContext);
            for (int i = 0; i < aggregators.length; i++) {
                if (!(aggregators[i] instanceof GlobalAggregator)) {
                    Aggregator aggregator = aggregators[i];
                    aggregatorCollectors.add(aggregator);
                }
            }
            context.aggregations().aggregators(aggregators);
        }
        aggregatorCollector = BucketCollector.wrap(aggregatorCollectors);
        aggregatorCollector.preCollection();
    }

    public void postMatch(int doc) throws IOException {
        aggregatorLeafCollector.collect(doc);
    }

    @Override
    public void setScorer(Scorer scorer) throws IOException {
        aggregatorLeafCollector.setScorer(scorer);
    }

    @Override
    public boolean needsScores() {
        return aggregatorCollector.needsScores();
    }

    @Override
    public void doSetNextReader(LeafReaderContext context) throws IOException {
        // we use the UID because id might not be indexed
        values = uidFieldData.load(context).getBytesValues();
        aggregatorLeafCollector = aggregatorCollector.getLeafCollector(context);
    }

    static Match match(ESLogger logger, PercolateContext context, HighlightPhase highlightPhase, boolean isNestedDoc) throws IOException {
        return new Match(logger, context, highlightPhase, isNestedDoc);
    }

    static Count count(ESLogger logger, PercolateContext context, boolean isNestedDoc) throws IOException {
        return new Count(logger, context, isNestedDoc);
    }

    static MatchAndScore matchAndScore(ESLogger logger, PercolateContext context, HighlightPhase highlightPhase, boolean isNestedDoc) throws IOException {
        return new MatchAndScore(logger, context, highlightPhase, isNestedDoc);
    }

    static MatchAndSort matchAndSort(ESLogger logger, PercolateContext context, boolean isNestedDoc) throws IOException {
        return new MatchAndSort(logger, context, isNestedDoc);
    }


    protected final Query getQuery(int doc) {
        values.setDocument(doc);
        final int numValues = values.count();
        if (numValues == 0) {
            return null;
        }
        assert numValues == 1;
        current = Uid.splitUidIntoTypeAndId(values.valueAt(0))[1];
        return queries.get(current);
    }



    final static class Match extends QueryCollector {

        final PercolateContext context;
        final HighlightPhase highlightPhase;

        final List<BytesRef> matches = new ArrayList<>();
        final List<Map<String, HighlightField>> hls = new ArrayList<>();
        final boolean limit;
        final int size;
        long counter = 0;

        Match(ESLogger logger, PercolateContext context, HighlightPhase highlightPhase, boolean isNestedDoc) throws IOException {
            super(logger, context, isNestedDoc);
            this.limit = context.limit;
            this.size = context.size();
            this.context = context;
            this.highlightPhase = highlightPhase;
        }

        @Override
        public void collect(int doc) throws IOException {
            final Query query = getQuery(doc);
            if (query == null) {
                // log???
                return;
            }
            Query existsQuery = query;
            if (isNestedDoc) {
                existsQuery = new BooleanQuery.Builder()
                    .add(existsQuery, Occur.MUST)
                    .add(Queries.newNonNestedFilter(), Occur.FILTER)
                    .build();
            }
            // run the query
            try {
                if (context.highlight() != null) {
                    context.parsedQuery(new ParsedQuery(query));
                    context.hitContext().cache().clear();
                }

                if (Lucene.exists(searcher, existsQuery)) {
                    if (!limit || counter < size) {
                        matches.add(BytesRef.deepCopyOf(current));
                        if (context.highlight() != null) {
                            highlightPhase.hitExecute(context, context.hitContext());
                            hls.add(context.hitContext().hit().getHighlightFields());
                        }
                    }
                    counter++;
                    postMatch(doc);
                }
            } catch (IOException e) {
                logger.warn("[" + current.utf8ToString() + "] failed to execute query", e);
            }
        }

        long counter() {
            return counter;
        }

        List<BytesRef> matches() {
            return matches;
        }

        List<Map<String, HighlightField>> hls() {
            return hls;
        }
    }

    final static class MatchAndSort extends QueryCollector {

        private final TopScoreDocCollector topDocsCollector;
        private LeafCollector topDocsLeafCollector;

        MatchAndSort(ESLogger logger, PercolateContext context, boolean isNestedDoc) throws IOException {
            super(logger, context, isNestedDoc);
            // TODO: Use TopFieldCollector.create(...) for ascending and descending scoring?
            topDocsCollector = TopScoreDocCollector.create(context.size());
        }

        @Override
        public boolean needsScores() {
            return super.needsScores() || topDocsCollector.needsScores();
        }

        @Override
        public void collect(int doc) throws IOException {
            final Query query = getQuery(doc);
            if (query == null) {
                // log???
                return;
            }
            Query existsQuery = query;
            if (isNestedDoc) {
                existsQuery = new BooleanQuery.Builder()
                    .add(existsQuery, Occur.MUST)
                    .add(Queries.newNonNestedFilter(), Occur.FILTER)
                    .build();
            }
            // run the query
            try {
                if (Lucene.exists(searcher, existsQuery)) {
                    topDocsLeafCollector.collect(doc);
                    postMatch(doc);
                }
            } catch (IOException e) {
                logger.warn("[" + current.utf8ToString() + "] failed to execute query", e);
            }
        }

        @Override
        public void doSetNextReader(LeafReaderContext context) throws IOException {
            super.doSetNextReader(context);
            topDocsLeafCollector = topDocsCollector.getLeafCollector(context);
        }

        @Override
        public void setScorer(Scorer scorer) throws IOException {
            topDocsLeafCollector.setScorer(scorer);
        }

        TopDocs topDocs() {
            return topDocsCollector.topDocs();
        }

    }

    final static class MatchAndScore extends QueryCollector {

        final PercolateContext context;
        final HighlightPhase highlightPhase;

        final List<BytesRef> matches = new ArrayList<>();
        final List<Map<String, HighlightField>> hls = new ArrayList<>();
        // TODO: Use thread local in order to cache the scores lists?
        final FloatArrayList scores = new FloatArrayList();
        final boolean limit;
        final int size;
        long counter = 0;

        private Scorer scorer;

        MatchAndScore(ESLogger logger, PercolateContext context, HighlightPhase highlightPhase, boolean isNestedDoc) throws IOException {
            super(logger, context, isNestedDoc);
            this.limit = context.limit;
            this.size = context.size();
            this.context = context;
            this.highlightPhase = highlightPhase;
        }

        @Override
        public boolean needsScores() {
            return true;
        }

        @Override
        public void collect(int doc) throws IOException {
            final Query query = getQuery(doc);
            if (query == null) {
                // log???
                return;
            }
            Query existsQuery = query;
            if (isNestedDoc) {
                existsQuery = new BooleanQuery.Builder()
                    .add(existsQuery, Occur.MUST)
                    .add(Queries.newNonNestedFilter(), Occur.FILTER)
                    .build();
            }
            // run the query
            try {
                if (context.highlight() != null) {
                    context.parsedQuery(new ParsedQuery(query));
                    context.hitContext().cache().clear();
                }
                if (Lucene.exists(searcher, existsQuery)) {
                    if (!limit || counter < size) {
                        matches.add(BytesRef.deepCopyOf(current));
                        scores.add(scorer.score());
                        if (context.highlight() != null) {
                            highlightPhase.hitExecute(context, context.hitContext());
                            hls.add(context.hitContext().hit().getHighlightFields());
                        }
                    }
                    counter++;
                    postMatch(doc);
                }
            } catch (IOException e) {
                logger.warn("[" + current.utf8ToString() + "] failed to execute query", e);
            }
        }

        @Override
        public void setScorer(Scorer scorer) throws IOException {
            this.scorer = scorer;
        }

        long counter() {
            return counter;
        }

        List<BytesRef> matches() {
            return matches;
        }

        FloatArrayList scores() {
            return scores;
        }

        List<Map<String, HighlightField>> hls() {
            return hls;
        }
    }

    final static class Count extends QueryCollector {

        private long counter = 0;

        Count(ESLogger logger, PercolateContext context, boolean isNestedDoc) throws IOException {
            super(logger, context, isNestedDoc);
        }

        @Override
        public void collect(int doc) throws IOException {
            final Query query = getQuery(doc);
            if (query == null) {
                // log???
                return;
            }
            Query existsQuery = query;
            if (isNestedDoc) {
                existsQuery = new BooleanQuery.Builder()
                    .add(existsQuery, Occur.MUST)
                    .add(Queries.newNonNestedFilter(), Occur.FILTER)
                    .build();
            }
            // run the query
            try {
                if (Lucene.exists(searcher, existsQuery)) {
                    counter++;
                    postMatch(doc);
                }
            } catch (IOException e) {
                logger.warn("[" + current.utf8ToString() + "] failed to execute query", e);
            }
        }

        long counter() {
            return counter;
        }

    }

}
