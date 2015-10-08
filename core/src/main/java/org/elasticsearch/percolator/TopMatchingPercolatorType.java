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
import java.util.*;

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
    PercolatorService.ReduceResult reduce(List<PercolateShardResponse> shardResponses, HasContextAndHeaders headersContext) throws IOException {
        int requestedSize = shardResponses.get(0).requestedSize();
        TopDocs[] shardResults = new TopDocs[shardResponses.size()];
        long foundMatches = 0;
        for (int i = 0; i < shardResults.length; i++) {
            TopDocs shardResult = shardResponses.get(i).topDocs();
            foundMatches += shardResult.totalHits;
            shardResults[i] = shardResult;
        }
        TopDocs merged = TopDocs.merge(requestedSize, shardResults);
        PercolateResponse.Match[] matches = new PercolateResponse.Match[merged.scoreDocs.length];
        for (int i = 0; i < merged.scoreDocs.length; i++) {
            ScoreDoc doc = merged.scoreDocs[i];
            PercolateShardResponse shardResponse = shardResponses.get(doc.shardIndex);
            String id = shardResponse.ids().get(doc.doc);
            Map<String, HighlightField> hl = shardResponse.hls().get(doc.doc);
            matches[i] = new PercolateResponse.Match(new StringText(shardResponse.getIndex()), new StringText(id), doc.score, hl);
        }
        InternalAggregations reducedAggregations = reduceAggregations(shardResponses, headersContext);
        return new PercolatorService.ReduceResult(foundMatches, matches, reducedAggregations);
    }

    @Override
    TopScoreDocCollector getCollector(int size) {
        return TopScoreDocCollector.create(size);
    }

    @Override
    PercolateShardResponse processResults(PercolateContext context, PercolatorQueriesRegistry registry, TopScoreDocCollector collector) throws IOException {
        TopDocs topDocs = collector.topDocs();
        Map<Integer, String> ids = new HashMap<>();
        Map<Integer, Map<String, HighlightField>> hls = new HashMap<>();

        for (ScoreDoc scoreDoc : topDocs.scoreDocs) {
            int segmentIdx = ReaderUtil.subIndex(scoreDoc.doc, context.searcher().getIndexReader().leaves());
            LeafReaderContext atomicReaderContext = context.searcher().getIndexReader().leaves().get(segmentIdx);
            final int segmentDocId = scoreDoc.doc - atomicReaderContext.docBase;
            SingleFieldsVisitor fieldsVisitor = new SingleFieldsVisitor(UidFieldMapper.NAME);
            atomicReaderContext.reader().document(segmentDocId, fieldsVisitor);
            String id = fieldsVisitor.uid().id();
            ids.put(scoreDoc.doc, id);
            if (context.highlight() != null) {
                Query query = registry.getPercolateQueries().get(new BytesRef(id));
                context.parsedQuery(new ParsedQuery(query));
                context.hitContext().cache().clear();
                highlightPhase.hitExecute(context, context.hitContext());
                hls.put(scoreDoc.doc, context.hitContext().hit().getHighlightFields());
            }
        }
        return new PercolateShardResponse(topDocs, ids, hls, context);
    }

}
