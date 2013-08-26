/*
 * Licensed to Elastic Search and Shay Banon under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. Elastic Search licenses this
 * file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.elasticsearch.percolator;

import com.google.common.collect.ImmutableMap;
import gnu.trove.list.array.TFloatArrayList;
import org.apache.lucene.index.AtomicReaderContext;
import org.apache.lucene.search.*;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.common.logging.ESLogger;
import org.elasticsearch.common.lucene.HashedBytesRef;
import org.elasticsearch.common.lucene.Lucene;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.index.fielddata.BytesValues;
import org.elasticsearch.index.fielddata.FieldDataType;
import org.elasticsearch.index.fielddata.IndexFieldData;
import org.elasticsearch.index.mapper.FieldMapper;
import org.elasticsearch.index.mapper.internal.IdFieldMapper;
import org.elasticsearch.index.query.ParsedQuery;
import org.elasticsearch.search.highlight.HighlightField;
import org.elasticsearch.search.highlight.HighlightPhase;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentMap;

/**
 */
abstract class QueryCollector extends Collector {

    final IndexFieldData idFieldData;
    final IndexSearcher searcher;
    final ConcurrentMap<HashedBytesRef, Query> queries;
    final ESLogger logger;

    final Lucene.ExistsCollector collector = new Lucene.ExistsCollector();
    final HashedBytesRef spare = new HashedBytesRef(new BytesRef());

    BytesValues values;

    QueryCollector(ESLogger logger, PercolateContext context) {
        this.logger = logger;
        this.queries = context.percolateQueries();
        this.searcher = context.docSearcher();
        this.idFieldData = context.fieldData().getForField(
                new FieldMapper.Names(IdFieldMapper.NAME),
                new FieldDataType("string", ImmutableSettings.builder().put("format", "paged_bytes"))
        );
    }

    @Override
    public void setScorer(Scorer scorer) throws IOException {
    }

    @Override
    public void setNextReader(AtomicReaderContext context) throws IOException {
        // we use the UID because id might not be indexed
        values = idFieldData.load(context).getBytesValues();
    }

    @Override
    public boolean acceptsDocsOutOfOrder() {
        return true;
    }


    static Match match(ESLogger logger, PercolateContext context, HighlightPhase highlightPhase) {
        return new Match(logger, context, highlightPhase);
    }

    static Count count(ESLogger logger, PercolateContext context) {
        return new Count(logger, context);
    }

    static MatchAndScore matchAndScore(ESLogger logger, PercolateContext context, HighlightPhase highlightPhase) {
        return new MatchAndScore(logger, context, highlightPhase);
    }

    static MatchAndSort matchAndSort(ESLogger logger, PercolateContext context) {
        return new MatchAndSort(logger, context);
    }

    final static class Match extends QueryCollector {

        final PercolateContext context;
        final HighlightPhase highlightPhase;

        final List<BytesRef> matches = new ArrayList<BytesRef>();
        final List<Map<String, HighlightField>> hls = new ArrayList<Map<String, HighlightField>>();
        final boolean limit;
        final int size;
        long counter = 0;

        Match(ESLogger logger, PercolateContext context, HighlightPhase highlightPhase) {
            super(logger, context);
            this.limit = context.limit;
            this.size = context.size;
            this.context = context;
            this.highlightPhase = highlightPhase;
        }

        @Override
        public void collect(int doc) throws IOException {
            spare.hash = values.getValueHashed(doc, spare.bytes);
            Query query = queries.get(spare);
            if (query == null) {
                // log???
                return;
            }
            // run the query
            try {
                collector.reset();
                if (context.highlight() != null) {
                    context.parsedQuery(new ParsedQuery(query, ImmutableMap.<String, Filter>of()));
                    context.hitContext().cache().clear();
                }

                searcher.search(query, collector);
                if (collector.exists()) {
                    if (!limit || counter < size) {
                        matches.add(values.makeSafe(spare.bytes));
                        if (context.highlight() != null) {
                            highlightPhase.hitExecute(context, context.hitContext());
                            hls.add(context.hitContext().hit().getHighlightFields());
                        }
                    }
                    counter++;
                }
            } catch (IOException e) {
                logger.warn("[" + spare.bytes.utf8ToString() + "] failed to execute query", e);
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

        MatchAndSort(ESLogger logger, PercolateContext context) {
            super(logger, context);
            // TODO: Use TopFieldCollector.create(...) for ascending and decending scoring?
            topDocsCollector = TopScoreDocCollector.create(context.size, false);
        }

        @Override
        public void collect(int doc) throws IOException {
            spare.hash = values.getValueHashed(doc, spare.bytes);
            Query query = queries.get(spare);
            if (query == null) {
                // log???
                return;
            }
            // run the query
            try {
                collector.reset();
                searcher.search(query, collector);
                if (collector.exists()) {
                    topDocsCollector.collect(doc);
                }
            } catch (IOException e) {
                logger.warn("[" + spare.bytes.utf8ToString() + "] failed to execute query", e);
            }
        }

        @Override
        public void setNextReader(AtomicReaderContext context) throws IOException {
            super.setNextReader(context);
            topDocsCollector.setNextReader(context);
        }

        @Override
        public void setScorer(Scorer scorer) throws IOException {
            topDocsCollector.setScorer(scorer);
        }

        TopDocs topDocs() {
            return topDocsCollector.topDocs();
        }

    }

    final static class MatchAndScore extends QueryCollector {

        final PercolateContext context;
        final HighlightPhase highlightPhase;

        final List<BytesRef> matches = new ArrayList<BytesRef>();
        final List<Map<String, HighlightField>> hls = new ArrayList<Map<String, HighlightField>>();
        // TODO: Use thread local in order to cache the scores lists?
        final TFloatArrayList scores = new TFloatArrayList();
        final boolean limit;
        final int size;
        long counter = 0;

        private Scorer scorer;

        MatchAndScore(ESLogger logger, PercolateContext context, HighlightPhase highlightPhase) {
            super(logger, context);
            this.limit = context.limit;
            this.size = context.size;
            this.context = context;
            this.highlightPhase = highlightPhase;
        }

        @Override
        public void collect(int doc) throws IOException {
            spare.hash = values.getValueHashed(doc, spare.bytes);
            Query query = queries.get(spare);
            if (query == null) {
                // log???
                return;
            }
            // run the query
            try {
                collector.reset();
                if (context.highlight() != null) {
                    context.parsedQuery(new ParsedQuery(query, ImmutableMap.<String, Filter>of()));
                    context.hitContext().cache().clear();
                }
                searcher.search(query, collector);
                if (collector.exists()) {
                    if (!limit || counter < size) {
                        matches.add(values.makeSafe(spare.bytes));
                        scores.add(scorer.score());
                        if (context.highlight() != null) {
                            highlightPhase.hitExecute(context, context.hitContext());
                            hls.add(context.hitContext().hit().getHighlightFields());
                        }
                    }
                    counter++;
                }
            } catch (IOException e) {
                logger.warn("[" + spare.bytes.utf8ToString() + "] failed to execute query", e);
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

        TFloatArrayList scores() {
            return scores;
        }

        List<Map<String, HighlightField>> hls() {
            return hls;
        }
    }

    final static class Count extends QueryCollector {

        private long counter = 0;

        Count(ESLogger logger, PercolateContext context) {
            super(logger, context);
        }

        @Override
        public void collect(int doc) throws IOException {
            spare.hash = values.getValueHashed(doc, spare.bytes);
            Query query = queries.get(spare);
            if (query == null) {
                // log???
                return;
            }
            // run the query
            try {
                collector.reset();
                searcher.search(query, collector);
                if (collector.exists()) {
                    counter++;
                }
            } catch (IOException e) {
                logger.warn("[" + spare.bytes.utf8ToString() + "] failed to execute query", e);
            }
        }

        long counter() {
            return counter;
        }

    }

}
