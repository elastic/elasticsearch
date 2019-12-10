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

package org.elasticsearch.search.aggregations.bucket.significant;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.miscellaneous.DeDuplicatingTokenFilter;
import org.apache.lucene.analysis.miscellaneous.DuplicateByteSequenceSpotter;
import org.apache.lucene.analysis.tokenattributes.CharTermAttribute;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.BytesRefBuilder;
import org.elasticsearch.common.lease.Releasables;
import org.elasticsearch.common.util.BytesRefHash;
import org.elasticsearch.index.mapper.MappedFieldType;
import org.elasticsearch.search.DocValueFormat;
import org.elasticsearch.search.aggregations.Aggregator;
import org.elasticsearch.search.aggregations.AggregatorFactories;
import org.elasticsearch.search.aggregations.LeafBucketCollector;
import org.elasticsearch.search.aggregations.LeafBucketCollectorBase;
import org.elasticsearch.search.aggregations.bucket.BucketsAggregator;
import org.elasticsearch.search.aggregations.bucket.significant.heuristics.SignificanceHeuristic;
import org.elasticsearch.search.aggregations.bucket.terms.IncludeExclude;
import org.elasticsearch.search.aggregations.bucket.terms.IncludeExclude.StringFilter;
import org.elasticsearch.search.aggregations.bucket.terms.TermsAggregator.BucketCountThresholds;
import org.elasticsearch.search.aggregations.pipeline.PipelineAggregator;
import org.elasticsearch.search.internal.ContextIndexSearcher;
import org.elasticsearch.search.internal.SearchContext;
import org.elasticsearch.search.lookup.SourceLookup;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static java.util.Collections.emptyList;

public class SignificantTextAggregator extends BucketsAggregator {

    private final StringFilter includeExclude;
    protected final BucketCountThresholds bucketCountThresholds;
    protected long numCollectedDocs;
    private final BytesRefHash bucketOrds;
    private final SignificanceHeuristic significanceHeuristic;
    private SignificantTextAggregatorFactory termsAggFactory;
    private final DocValueFormat format = DocValueFormat.RAW;
    private final String fieldName;
    private final String[] sourceFieldNames;
    private DuplicateByteSequenceSpotter dupSequenceSpotter = null ;
    private long lastTrieSize;
    private static final int MEMORY_GROWTH_REPORTING_INTERVAL_BYTES = 5000;



    public SignificantTextAggregator(String name, AggregatorFactories factories,
            SearchContext context, Aggregator parent, List<PipelineAggregator> pipelineAggregators,
            BucketCountThresholds bucketCountThresholds, IncludeExclude.StringFilter includeExclude,
            SignificanceHeuristic significanceHeuristic, SignificantTextAggregatorFactory termsAggFactory,
            String fieldName, String [] sourceFieldNames, boolean filterDuplicateText,
            Map<String, Object> metaData) throws IOException {
        super(name, factories, context, parent, pipelineAggregators, metaData);
        this.bucketCountThresholds = bucketCountThresholds;
        this.includeExclude = includeExclude;
        this.significanceHeuristic = significanceHeuristic;
        this.termsAggFactory = termsAggFactory;
        this.fieldName = fieldName;
        this.sourceFieldNames = sourceFieldNames;
        bucketOrds = new BytesRefHash(1, context.bigArrays());
        if(filterDuplicateText){
            dupSequenceSpotter = new DuplicateByteSequenceSpotter();
            lastTrieSize = dupSequenceSpotter.getEstimatedSizeInBytes();
        }
    }




    @Override
    public LeafBucketCollector getLeafCollector(LeafReaderContext ctx,
            final LeafBucketCollector sub) throws IOException {
        final BytesRefBuilder previous = new BytesRefBuilder();
        return new LeafBucketCollectorBase(sub, null) {

            @Override
            public void collect(int doc, long bucket) throws IOException {
                collectFromSource(doc, bucket, fieldName, sourceFieldNames);
                numCollectedDocs++;
                if (dupSequenceSpotter != null) {
                    dupSequenceSpotter.startNewSequence();
                }
            }

            private void processTokenStream(int doc, long bucket, TokenStream ts, BytesRefHash inDocTerms, String fieldText)
                    throws IOException{
                if (dupSequenceSpotter != null) {
                    ts = new DeDuplicatingTokenFilter(ts, dupSequenceSpotter);
                }
                CharTermAttribute termAtt = ts.addAttribute(CharTermAttribute.class);
                ts.reset();
                try {
                    while (ts.incrementToken()) {
                        if (dupSequenceSpotter != null) {
                            long newTrieSize = dupSequenceSpotter.getEstimatedSizeInBytes();
                            long growth = newTrieSize - lastTrieSize;
                            // Only update the circuitbreaker after
                            if (growth > MEMORY_GROWTH_REPORTING_INTERVAL_BYTES) {
                                addRequestCircuitBreakerBytes(growth);
                                lastTrieSize = newTrieSize;
                            }
                        }
                        previous.clear();
                        previous.copyChars(termAtt);
                        BytesRef bytes = previous.get();
                        if (inDocTerms.add(bytes) >= 0) {
                            if (includeExclude == null || includeExclude.accept(bytes)) {
                                long bucketOrdinal = bucketOrds.add(bytes);
                                if (bucketOrdinal < 0) { // already seen
                                    bucketOrdinal = -1 - bucketOrdinal;
                                    collectExistingBucket(sub, doc, bucketOrdinal);
                                } else {
                                    collectBucket(sub, doc, bucketOrdinal);
                                }
                            }
                        }
                    }

                } finally{
                    ts.close();
                }
            }

            private void collectFromSource(int doc, long bucket, String indexedFieldName, String[] sourceFieldNames) throws IOException {
                MappedFieldType fieldType = context.getQueryShardContext().fieldMapper(indexedFieldName);
                if(fieldType == null){
                    throw new IllegalArgumentException("Aggregation [" + name + "] cannot process field ["+indexedFieldName
                            +"] since it is not present");
                }

                SourceLookup sourceLookup = context.lookup().source();
                sourceLookup.setSegmentAndDocument(ctx, doc);
                BytesRefHash inDocTerms = new BytesRefHash(256, context.bigArrays());

                try {
                    for (String sourceField : sourceFieldNames) {
                        List<Object> textsToHighlight = sourceLookup.extractRawValues(sourceField);
                        textsToHighlight = textsToHighlight.stream().map(obj -> {
                            if (obj instanceof BytesRef) {
                                return fieldType.valueForDisplay(obj).toString();
                            } else {
                                return obj;
                            }
                        }).collect(Collectors.toList());

                        Analyzer analyzer = fieldType.indexAnalyzer();
                        for (Object fieldValue : textsToHighlight) {
                            String fieldText = fieldValue.toString();
                            TokenStream ts = analyzer.tokenStream(indexedFieldName, fieldText);
                            processTokenStream(doc, bucket, ts, inDocTerms, fieldText);
                        }
                    }
                } finally{
                    Releasables.close(inDocTerms);
                }
            }
        };
    }

    @Override
    public SignificantStringTerms buildAggregation(long owningBucketOrdinal) throws IOException {
        assert owningBucketOrdinal == 0;

        final int size = (int) Math.min(bucketOrds.size(), bucketCountThresholds.getShardSize());
        long supersetSize = termsAggFactory.getSupersetNumDocs();
        long subsetSize = numCollectedDocs;

        BucketSignificancePriorityQueue<SignificantStringTerms.Bucket> ordered = new BucketSignificancePriorityQueue<>(size);
        SignificantStringTerms.Bucket spare = null;
        for (int i = 0; i < bucketOrds.size(); i++) {
            final int docCount = bucketDocCount(i);
            if (docCount < bucketCountThresholds.getShardMinDocCount()) {
                continue;
            }

            if (spare == null) {
                spare = new SignificantStringTerms.Bucket(new BytesRef(), 0, 0, 0, 0, null, format, 0);
            }

            bucketOrds.get(i, spare.termBytes);
            spare.subsetDf = docCount;
            spare.subsetSize = subsetSize;
            spare.supersetDf = termsAggFactory.getBackgroundFrequency(spare.termBytes);
            spare.supersetSize = supersetSize;
            // During shard-local down-selection we use subset/superset stats
            // that are for this shard only
            // Back at the central reducer these properties will be updated with
            // global stats
            spare.updateScore(significanceHeuristic);

            spare.bucketOrd = i;
            spare = ordered.insertWithOverflow(spare);
            if (spare == null) {
                consumeBucketsAndMaybeBreak(1);
            }
        }

        final SignificantStringTerms.Bucket[] list = new SignificantStringTerms.Bucket[ordered.size()];
        for (int i = ordered.size() - 1; i >= 0; i--) {
            final SignificantStringTerms.Bucket bucket = ordered.pop();
            // the terms are owned by the BytesRefHash, we need to pull a copy since the BytesRef hash data may be recycled at some point
            bucket.termBytes = BytesRef.deepCopyOf(bucket.termBytes);
            bucket.aggregations = bucketAggregations(bucket.bucketOrd);
            list[i] = bucket;
        }

        return new SignificantStringTerms( name, bucketCountThresholds.getRequiredSize(),
                bucketCountThresholds.getMinDocCount(), pipelineAggregators(),
                metaData(), format, subsetSize, supersetSize, significanceHeuristic, Arrays.asList(list));
    }


    @Override
    public SignificantStringTerms buildEmptyAggregation() {
        // We need to account for the significance of a miss in our global stats - provide corpus size as context
        ContextIndexSearcher searcher = context.searcher();
        IndexReader topReader = searcher.getIndexReader();
        int supersetSize = topReader.numDocs();
        return new SignificantStringTerms(name, bucketCountThresholds.getRequiredSize(), bucketCountThresholds.getMinDocCount(),
                pipelineAggregators(), metaData(), format, 0, supersetSize, significanceHeuristic, emptyList());
    }

    @Override
    public void doClose() {
        Releasables.close(bucketOrds, termsAggFactory);
    }

}
