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
import org.apache.lucene.analysis.miscellaneous.LimitTokenCountFilter;
import org.apache.lucene.analysis.tokenattributes.CharTermAttribute;
import org.apache.lucene.index.AtomicReaderContext;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.search.*;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.Version;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.ExceptionsHelper;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.io.FastStringReader;
import org.elasticsearch.common.lease.Releasables;
import org.elasticsearch.common.lucene.ScorerAware;
import org.elasticsearch.index.analysis.AnalysisService;
import org.elasticsearch.index.analysis.ByteStreamDuplicateSequenceSpotter;
import org.elasticsearch.index.analysis.DeDuplicatingTokenFilter;
import org.elasticsearch.index.fieldvisitor.CustomFieldsVisitor;
import org.elasticsearch.index.fieldvisitor.FieldsVisitor;
import org.elasticsearch.index.fieldvisitor.UidAndSourceFieldsVisitor;
import org.elasticsearch.index.mapper.FieldMappers;
import org.elasticsearch.search.aggregations.Aggregator;
import org.elasticsearch.search.aggregations.AggregatorFactories;
import org.elasticsearch.search.aggregations.bucket.significant.SignificantStringTerms.Bucket;
import org.elasticsearch.search.aggregations.bucket.significant.heuristics.SignificanceHeuristic;
import org.elasticsearch.search.aggregations.bucket.terms.TermsAggregator;
import org.elasticsearch.search.aggregations.bucket.terms.support.IncludeExclude;
import org.elasticsearch.search.aggregations.support.AggregationContext;
import org.elasticsearch.search.aggregations.support.FieldContext;
import org.elasticsearch.search.internal.ContextIndexSearcher;
import org.elasticsearch.search.internal.SearchContext;
import org.elasticsearch.search.query.QueryPhaseExecutionException;

import java.io.IOException;
import java.util.*;

/**
 * An aggregator of significant string values based on samples of top-matching documents.
 * Sample docs can be cleansed of duplicate content for better results e.g. when the content
 * might be many emails with copies of prior content or many tweets which include retweets.
 * 
 * TODO - what to do about any potential child aggs? Use the deferring mechanism in the base class
 * until collection completed here or simply throw an exception saying child aggs are not permitted?
 * 
 */
public class SignificantStringTermsSamplingAggregator extends TermsAggregator implements ScorerAware {

    protected final SignificantTermsAggregatorFactory termsAggFactory;
    private TopDocsCollector topDocsCollector;
    private Scorer currentScorer;
    private AtomicReaderContext currentContext;
    private String fieldName;
    private IncludeExclude includeExclude;
    private SampleSettings samplerSettings;
    private SignificanceHeuristic significantHeuristic;

    public SignificantStringTermsSamplingAggregator(String name, AggregatorFactories factories, FieldContext fieldContext,
            long estimatedBucketCount, BucketCountThresholds bucketCountThresholds, IncludeExclude includeExclude,
            AggregationContext aggregationContext, Aggregator parent, SignificantTermsAggregatorFactory termsAggFactory,
            SignificanceHeuristic significanceHeuristic, SampleSettings samplerSettings) {
        super(name, BucketAggregationMode.PER_BUCKET, factories, estimatedBucketCount, aggregationContext, parent, bucketCountThresholds,
                null, SubAggCollectionMode.DEPTH_FIRST);
        this.termsAggFactory = termsAggFactory;
        this.includeExclude = includeExclude;
        this.samplerSettings = samplerSettings;
        this.significantHeuristic = significanceHeuristic;
        this.fieldName = fieldContext.field();
        context.registerScorerAware(this);
    }

    @Override
    public void collect(int doc, long owningBucketOrdinal) throws IOException {
        if (topDocsCollector == null) {
            topDocsCollector = TopScoreDocCollector.create(samplerSettings.getNumDocsPerShard(), false);
            topDocsCollector.setNextReader(currentContext);
            topDocsCollector.setScorer(currentScorer);
        }
        topDocsCollector.collect(doc);
    }
    
    @Override
    public void setNextReader(AtomicReaderContext reader) {
        currentContext = reader;
        if (topDocsCollector != null) {
            try {
                topDocsCollector.setNextReader(currentContext);
            } catch (IOException e) {
                throw ExceptionsHelper.convertToElastic(e);
            }
        }
    }   

    @Override
    public SignificantStringTerms buildAggregation(long owningBucketOrdinal) {
        assert owningBucketOrdinal == 0;
        if (topDocsCollector == null) {
            return buildEmptyAggregation();
        }
        final int maxNumTermsToReturn = (int) bucketCountThresholds.getShardSize();
        TopDocs topDocs = topDocsCollector.topDocs();
        ScoreDoc[] sd = topDocs.scoreDocs;

        List<Bucket> sampleTokenStats = getTermStatsFromForegroundSample(sd);
        
        //Complete each term's stats using background frequency and calc score
        long supersetSize = termsAggFactory.prepareBackground(context);
        long subsetSize = sd.length;
        BucketSignificancePriorityQueue ordered = new BucketSignificancePriorityQueue(maxNumTermsToReturn);
        int bucketNum = 0;
        long shardMinDocCount = bucketCountThresholds.getShardMinDocCount();
        for (Bucket bucket : sampleTokenStats) {
            if (bucket.subsetDf >= shardMinDocCount) {
                if (includeExclude != null && !includeExclude.accept(bucket.termBytes)) {
                    continue;
                }
                bucket.subsetSize = subsetSize;
                bucket.supersetDf = termsAggFactory.getBackgroundFrequency(bucket.termBytes);
                bucket.supersetSize = supersetSize;
                bucket.updateScore(significantHeuristic);
                bucketNum++;
                bucket.bucketOrd = bucketNum;
                ordered.insertWithOverflow(bucket);
            }
        }
        final InternalSignificantTerms.Bucket[] list = new InternalSignificantTerms.Bucket[ordered.size()];
        for (int i = ordered.size() - 1; i >= 0; i--) {
            final SignificantStringTerms.Bucket bucket = (SignificantStringTerms.Bucket) ordered.pop();
            bucket.aggregations = bucketAggregations(bucket.bucketOrd);
            list[i] = bucket;
        }
        return new SignificantStringTerms(subsetSize, supersetSize, name, bucketCountThresholds.getRequiredSize(),
                bucketCountThresholds.getMinDocCount(), significantHeuristic, Arrays.asList(list));
      }

    /**
     * Loads a sample of top-matching content, applying any required truncation/de-duping
     * and provides "foreground" or "subset" stats for resulting Buckets
     * @param sd The top-scoring documents
     * @return The top terms' stats, sorted by term
     */
    private List<Bucket> getTermStatsFromForegroundSample(ScoreDoc[] sd) {
        // Uses TreeMap to maintain sorted sequence of terms for faster
        // subsequent lookup of background frequencies
        Map<String, SignificantStringTerms.Bucket> result = new HashMap<>();
        SearchContext searchContext = SearchContext.current();
        ByteStreamDuplicateSequenceSpotter byteStreamDuplicateSequenceSpotter = null;
        if (samplerSettings.getDuplicateParagraphLengthInWords() > SampleSettings.NO_DUPLICATE_DETECTION) {
            int bufferSizeInBytes = Math.min(samplerSettings.numRecordedTokens,
                    (samplerSettings.getNumDocsPerShard() * samplerSettings.getMaxTokensParsedPerDocument()));
            // TODO move ByteStreamDuplicateSequenceSpotter to using BigArrays
            // for circuit-breaker/recycling benefits over raw byte[] impl?
            // A generic byte array impl seems useful to maintain more broadly
            // but once we've ironed out all
            // the potential issues in this maybe port it to using
            // BigArrays-backed arrays.
            byteStreamDuplicateSequenceSpotter = new ByteStreamDuplicateSequenceSpotter(bufferSizeInBytes);
        }
        FieldTokenStreamProvider provider = new FieldTokenStreamProvider(fieldName, searchContext);
        Set<String> thisDocsUniqueTokens = new HashSet<>();
        for (int i = 0; i < sd.length; i++) {
            int docId = sd[i].doc;
            thisDocsUniqueTokens.clear();
            TokenStream stream = null;
            try {
                stream = provider.getTokenStream(docId);
                if (stream == null) {
                    continue;
                }
                if (samplerSettings.getMaxTokensParsedPerDocument() > 0) {
                    stream = new LimitTokenCountFilter(stream, samplerSettings.getMaxTokensParsedPerDocument());
                }
                if (byteStreamDuplicateSequenceSpotter != null) {
                    // Add a filter to remove duplicate content using the
                    // byteStreamDuplicateSequenceSpotter
                    stream = new DeDuplicatingTokenFilter(Version.LUCENE_4_9, stream, byteStreamDuplicateSequenceSpotter,
                            samplerSettings.getDuplicateParagraphLengthInWords());
                }
                stream.reset();
                CharTermAttribute term = stream.addAttribute(CharTermAttribute.class);
                // TODO - how to get ByteRefs rather than strings?
                while (stream.incrementToken()) {
                    thisDocsUniqueTokens.add(term.toString());
                }
                stream.end();
            } catch (IOException e) {
                throw new ElasticsearchException("failed to analyze field " + fieldName, e);
            } finally {
                if (stream != null) {
                    try {
                        stream.close();
                    } catch (IOException e) {
                        // ignore
                    }
                }
            }
            // Now we have a de-duped and unique set of tokens, add to our
            // sample's stats
            for (String t : thisDocsUniqueTokens) {
                Bucket stats = result.get(t);
                if (stats == null) {
                    stats = new Bucket(new BytesRef(t), 0, sd.length, 0, 0, null);
                    result.put(t, stats);
                }
                stats.subsetDf++;
            }
        }
        // Sort the buckets for faster lookup of background term frequencies
        ArrayList<Bucket> sortedResults = new ArrayList<>(result.values());
        Collections.sort(sortedResults, new Comparator<Bucket>() {
            @Override
            public int compare(Bucket o1, Bucket o2) {
                return o1.compareTerm(o2);
            }
        });
        return sortedResults;
    }
    
    // TODO break this out as a utility class (poss usable by MLT) and add
    // support for TermVectors if present (although I have heard they are not always faster 
    // than re-analysis)
    static class FieldTokenStreamProvider {
        private SearchContext searchContext;
        private String fieldName;
        private FieldsVisitor fieldsVisitor;
        private boolean sourceRequested = false;
        Analyzer analyzer;

        public FieldTokenStreamProvider(String fieldName, SearchContext searchContext) {
            this.searchContext = searchContext;
            this.fieldName = fieldName;

            AnalysisService analysisService = searchContext.analysisService();
            analyzer = analysisService.analyzer(fieldName);
            if (analyzer == null) {
                analyzer = analysisService.defaultIndexAnalyzer();
            }

            Set<String> fieldNames = null;
            List<String> extractFieldNames = null;
            FieldMappers x = searchContext.smartNameFieldMappers(fieldName);
            if ((x != null) && x.mapper().fieldType().stored()) {
                if (fieldNames == null) {
                    fieldNames = new HashSet<String>();
                }
                fieldNames.add(x.mapper().names().indexName());
            } else {
                if (extractFieldNames == null) {
                    extractFieldNames = new ArrayList<String>();
                }
                extractFieldNames.add(fieldName);
            }
            fieldsVisitor = null;
            if (fieldNames != null) {
                boolean loadSource = extractFieldNames != null;
                fieldsVisitor = new CustomFieldsVisitor(fieldNames, loadSource);
            } else if (extractFieldNames != null) {
                fieldsVisitor = new UidAndSourceFieldsVisitor();
                sourceRequested = true;
            }
        }

        TokenStream getTokenStream(int docId) throws IOException {
            // load stored fields
            fieldsVisitor.reset();
            try {
                searchContext.searcher().doc(docId, fieldsVisitor);
            } catch (IOException e) {
                throw new QueryPhaseExecutionException(searchContext, "Failed to load doc id [" + docId + "]", e);
            }
            fieldsVisitor.postProcess(searchContext.mapperService());
            String value = null;
            // Either the field is specially held as a stored field or is
            // squirrelled away in a JSON structure as part of a "source"
            // field
            if (sourceRequested) {
                BytesReference src = fieldsVisitor.source();
                if (src != null) {
                    // Load data vals from source (mostly copied from
                    // FetchPhase.java)
                    searchContext.lookup().source().setNextDocId(docId);
                    searchContext.lookup().source().setNextSource(src);
                    List<Object> values = searchContext.lookup().source().extractRawValues(fieldName);
                    if (!values.isEmpty()) {
                        StringBuilder sb = new StringBuilder();
                        for (Object val : values) {
                            if (val != null) {
                                sb.append(val);
                                sb.append(" ");
                            }
                        }
                        value = sb.toString();
                    }
                }
            } else {
                if (fieldsVisitor.fields() != null) {
                    for (Map.Entry<String, List<Object>> entry : fieldsVisitor.fields().entrySet()) {
                        StringBuilder sb = new StringBuilder();
                        for (Object val : entry.getValue()) {
                            sb.append(val);
                            sb.append(" ");
                        }
                        value = sb.toString();
                    }
                }
            }
            if (value != null) {
                return analyzer.tokenStream(fieldName, new FastStringReader(value));
            }
            return null;
        }
    }

    @Override
    public SignificantStringTerms buildEmptyAggregation() {
        // We need to account for the significance of a miss in our global stats
        // - provide corpus size as context
        ContextIndexSearcher searcher = context.searchContext().searcher();
        IndexReader topReader = searcher.getIndexReader();
        int supersetSize = topReader.numDocs();
        return new SignificantStringTerms(0, supersetSize, name, bucketCountThresholds.getRequiredSize(),
                bucketCountThresholds.getMinDocCount(), significantHeuristic, Collections.<InternalSignificantTerms.Bucket> emptyList());
    }
    
    @Override
    public void setScorer(Scorer scorer) {
        this.currentScorer = scorer;
        if (topDocsCollector != null) {
            try {
                topDocsCollector.setScorer(scorer);
            } catch (IOException e) {
                throw ExceptionsHelper.convertToElastic(e);
            }
        }
    }

    @Override
    public void doClose() {
        Releasables.close(termsAggFactory);
    }

    @Override
    public boolean shouldCollect() {
        return true;
    }

}

