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

package org.elasticsearch.search.fetch;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.ReaderUtil;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.ScoreMode;
import org.apache.lucene.search.Scorer;
import org.apache.lucene.search.TotalHits;
import org.apache.lucene.search.Weight;
import org.apache.lucene.util.BitSet;
import org.elasticsearch.ExceptionsHelper;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.collect.Tuple;
import org.elasticsearch.common.document.DocumentField;
import org.elasticsearch.common.lucene.search.Queries;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.common.xcontent.support.XContentMapValues;
import org.elasticsearch.index.fieldvisitor.CustomFieldsVisitor;
import org.elasticsearch.index.fieldvisitor.FieldsVisitor;
import org.elasticsearch.index.mapper.DocumentMapper;
import org.elasticsearch.index.mapper.MappedFieldType;
import org.elasticsearch.index.mapper.MapperService;
import org.elasticsearch.index.mapper.ObjectMapper;
import org.elasticsearch.index.mapper.SourceFieldMapper;
import org.elasticsearch.search.SearchContextSourcePrinter;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHits;
import org.elasticsearch.search.SearchShardTarget;
import org.elasticsearch.search.fetch.FetchSubPhase.HitContext;
import org.elasticsearch.search.fetch.subphase.FetchSourceContext;
import org.elasticsearch.search.fetch.subphase.InnerHitsContext;
import org.elasticsearch.search.fetch.subphase.InnerHitsPhase;
import org.elasticsearch.search.internal.SearchContext;
import org.elasticsearch.search.lookup.SourceLookup;
import org.elasticsearch.tasks.TaskCancelledException;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static java.util.Collections.emptyMap;

/**
 * Fetch phase of a search request, used to fetch the actual top matching documents to be returned to the client, identified
 * after reducing all of the matches returned by the query phase
 */
public class FetchPhase {
    private static final Logger LOGGER = LogManager.getLogger(FetchPhase.class);

    private final FetchSubPhase[] fetchSubPhases;

    public FetchPhase(List<FetchSubPhase> fetchSubPhases) {
        this.fetchSubPhases = fetchSubPhases.toArray(new FetchSubPhase[fetchSubPhases.size() + 1]);
        this.fetchSubPhases[fetchSubPhases.size()] = new InnerHitsPhase(this);
    }

    public void execute(SearchContext context) {
        if (LOGGER.isTraceEnabled()) {
            LOGGER.trace("{}", new SearchContextSourcePrinter(context));
        }

        try {
            DocIdToIndex[] docs = new DocIdToIndex[context.docIdsToLoadSize()];
            for (int index = 0; index < context.docIdsToLoadSize(); index++) {
                docs[index] = new DocIdToIndex(context.docIdsToLoad()[context.docIdsToLoadFrom() + index], index);
            }
            Arrays.sort(docs);

            SearchHit[] hits = new SearchHit[context.docIdsToLoadSize()];
            Map<String, Object> sharedCache = new HashMap<>();

            List<FetchSubPhaseProcessor> processors = new ArrayList<>();
            for (FetchSubPhase fsp : fetchSubPhases) {
                FetchSubPhaseProcessor processor = fsp.getProcessor(context);
                if (processor != null) {
                    processors.add(processor);
                }
            }

            int currentReaderIndex = -1;
            LeafReaderContext currentReaderContext = null;
            for (int index = 0; index < context.docIdsToLoadSize(); index++) {
                if (context.isCancelled()) {
                    throw new TaskCancelledException("cancelled");
                }
                int docId = docs[index].docId;
                int readerIndex = ReaderUtil.subIndex(docId, context.searcher().getIndexReader().leaves());
                if (currentReaderIndex != readerIndex) {
                    currentReaderContext = context.searcher().getIndexReader().leaves().get(readerIndex);
                    currentReaderIndex = readerIndex;
                    for (FetchSubPhaseProcessor processor : processors) {
                        processor.setNextReader(currentReaderContext);
                    }
                }
                assert currentReaderContext != null;

                HitContext hit = prepareHitContext(context, docId, currentReaderContext, sharedCache);
                for (FetchSubPhaseProcessor processor : processors) {
                    processor.process(hit);
                }
                hits[docs[index].index] = hit.hit();
            }
            if (context.isCancelled()) {
                throw new TaskCancelledException("cancelled");
            }

            TotalHits totalHits = context.queryResult().getTotalHits();
            context.fetchResult().hits(new SearchHits(hits, totalHits, context.queryResult().getMaxScore()));
        } catch (IOException e) {
            throw ExceptionsHelper.convertToElastic(e);
        }
    }

    static class DocIdToIndex implements Comparable<DocIdToIndex> {
        final int docId;
        final int index;

        DocIdToIndex(int docId, int index) {
            this.docId = docId;
            this.index = index;
        }

        @Override
        public int compareTo(DocIdToIndex o) {
            return Integer.compare(docId, o.docId);
        }
    }



    private int findRootDocumentIfNested(SearchContext context, LeafReaderContext subReaderContext, int subDocId) throws IOException {
        if (context.mapperService().hasNested()) {
            BitSet bits = context.bitsetFilterCache()
                .getBitSetProducer(Queries.newNonNestedFilter())
                .getBitSet(subReaderContext);
            if (!bits.get(subDocId)) {
                return bits.nextSetBit(subDocId);
            }
        }
        return -1;
    }

    private HitContext prepareHitContext(SearchContext context, int docId,
                                         LeafReaderContext subReaderContext, Map<String, Object> sharedCache) throws IOException {
        int rootDocId = findRootDocumentIfNested(context, subReaderContext, docId - subReaderContext.docBase);
        int subDocId = docId - subReaderContext.docBase;
        SearchHit hit = new SearchHit(docId, null, new HashMap<>(), new HashMap<>());
        return new HitContext(hit, subReaderContext, subDocId, rootDocId, context.searcher(), sharedCache);
    }

}
