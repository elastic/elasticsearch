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

import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.ReaderUtil;
import org.apache.lucene.search.*;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.action.percolate.PercolateResponse;
import org.elasticsearch.action.percolate.PercolateShardResponse;
import org.elasticsearch.common.HasContextAndHeaders;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.text.BytesText;
import org.elasticsearch.common.text.StringText;
import org.elasticsearch.common.text.Text;
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.index.fieldvisitor.SingleFieldsVisitor;
import org.elasticsearch.index.mapper.internal.UidFieldMapper;
import org.elasticsearch.index.percolator.PercolatorQueriesRegistry;
import org.elasticsearch.index.query.ParsedQuery;
import org.elasticsearch.script.ScriptService;
import org.elasticsearch.search.aggregations.InternalAggregations;
import org.elasticsearch.search.highlight.HighlightField;
import org.elasticsearch.search.highlight.HighlightPhase;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;

class TopMatchingPercolatorType extends PercolatorType<TopScoreDocCollector> {

    private final HighlightPhase highlightPhase;

    TopMatchingPercolatorType(BigArrays bigArrays, ScriptService scriptService, HighlightPhase highlightPhase) {
        super(bigArrays, scriptService);
        this.highlightPhase = highlightPhase;
    }

    @Override
    byte id() {
        return 0x06;
    }

    @Override
    PercolatorService.ReduceResult reduce(List<PercolateShardResponse> shardResults, HasContextAndHeaders headersContext) {
        long foundMatches = 0;
        int nonEmptyResponses = 0;
        int firstNonEmptyIndex = 0;
        for (int i = 0; i < shardResults.size(); i++) {
            PercolateShardResponse response = shardResults.get(i);
            foundMatches += response.count();
            if (response.matches().length != 0) {
                if (firstNonEmptyIndex == 0) {
                    firstNonEmptyIndex = i;
                }
                nonEmptyResponses++;
            }
        }

        int requestedSize = shardResults.get(0).requestedSize();

        // Use a custom impl of AbstractBigArray for Object[]?
        List<PercolateResponse.Match> finalMatches = new ArrayList<>(requestedSize);
        if (nonEmptyResponses == 1) {
            PercolateShardResponse response = shardResults.get(firstNonEmptyIndex);
            Text index = new StringText(response.getIndex());
            for (int i = 0; i < response.matches().length; i++) {
                float score = response.scores().length == 0 ? Float.NaN : response.scores()[i];
                Text match = new BytesText(new BytesArray(response.matches()[i]));
                if (!response.hls().isEmpty()) {
                    Map<String, HighlightField> hl = response.hls().get(i);
                    finalMatches.add(new PercolateResponse.Match(index, match, score, hl));
                } else {
                    finalMatches.add(new PercolateResponse.Match(index, match, score));
                }
            }
        } else {
            int[] slots = new int[shardResults.size()];
            while (true) {
                float lowestScore = Float.NEGATIVE_INFINITY;
                int requestIndex = -1;
                int itemIndex = -1;
                for (int i = 0; i < shardResults.size(); i++) {
                    int scoreIndex = slots[i];
                    float[] scores = shardResults.get(i).scores();
                    if (scoreIndex >= scores.length) {
                        continue;
                    }

                    float score = scores[scoreIndex];
                    int cmp = Float.compare(lowestScore, score);
                    // TODO: Maybe add a tie?
                    if (cmp < 0) {
                        requestIndex = i;
                        itemIndex = scoreIndex;
                        lowestScore = score;
                    }
                }

                // This means the shard matches have been exhausted and we should bail
                if (requestIndex == -1) {
                    break;
                }

                slots[requestIndex]++;

                PercolateShardResponse shardResponse = shardResults.get(requestIndex);
                Text index = new StringText(shardResponse.getIndex());
                Text match = new BytesText(new BytesArray(shardResponse.matches()[itemIndex]));
                float score = shardResponse.scores()[itemIndex];
                if (!shardResponse.hls().isEmpty()) {
                    Map<String, HighlightField> hl = shardResponse.hls().get(itemIndex);
                    finalMatches.add(new PercolateResponse.Match(index, match, score, hl));
                } else {
                    finalMatches.add(new PercolateResponse.Match(index, match, score));
                }
                if (finalMatches.size() == requestedSize) {
                    break;
                }
            }
        }

        assert !shardResults.isEmpty();
        InternalAggregations reducedAggregations = reduceAggregations(shardResults, headersContext);
        return new PercolatorService.ReduceResult(foundMatches, finalMatches.toArray(new PercolateResponse.Match[finalMatches.size()]), reducedAggregations);
    }

    @Override
    TopScoreDocCollector getCollector(int size) {
        return TopScoreDocCollector.create(size);
    }

    @Override
    PercolateShardResponse processResults(PercolateContext context, PercolatorQueriesRegistry registry, TopScoreDocCollector collector) throws IOException {
        TopDocs topDocs = collector.topDocs();
        long count = topDocs.totalHits;
        List<BytesRef> matches = new ArrayList<>(topDocs.scoreDocs.length);
        float[] scores = new float[topDocs.scoreDocs.length];
        boolean hl = context.highlight() != null;
        List<Map<String, HighlightField>> hls = hl ? new ArrayList<>(topDocs.scoreDocs.length) : Collections.emptyList();

        int i = 0;
        for (ScoreDoc scoreDoc : topDocs.scoreDocs) {
            int segmentIdx = ReaderUtil.subIndex(scoreDoc.doc, context.searcher().getIndexReader().leaves());
            LeafReaderContext atomicReaderContext = context.searcher().getIndexReader().leaves().get(segmentIdx);
            final int segmentDocId = scoreDoc.doc - atomicReaderContext.docBase;
            SingleFieldsVisitor fieldsVisitor = new SingleFieldsVisitor(UidFieldMapper.NAME);
            atomicReaderContext.reader().document(segmentDocId, fieldsVisitor);
            BytesRef id = new BytesRef(fieldsVisitor.uid().id());
            matches.add(id);
            if (hl) {
                Query query = registry.getPercolateQueries().get(id);
                context.parsedQuery(new ParsedQuery(query));
                context.hitContext().cache().clear();
                highlightPhase.hitExecute(context, context.hitContext());
                hls.add(i, context.hitContext().hit().getHighlightFields());
            }
            scores[i++] = scoreDoc.score;
        }
        return new PercolateShardResponse(matches.toArray(new BytesRef[matches.size()]), hls, count, scores, context);
    }

}
