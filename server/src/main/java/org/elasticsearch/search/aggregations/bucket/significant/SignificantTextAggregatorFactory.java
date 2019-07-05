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

import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.PostingsEnum;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.BooleanClause.Occur;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.common.lease.Releasable;
import org.elasticsearch.common.lucene.index.FilterableTermsEnum;
import org.elasticsearch.common.lucene.index.FreqTermsEnum;
import org.elasticsearch.index.mapper.MappedFieldType;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.search.DocValueFormat;
import org.elasticsearch.search.aggregations.Aggregator;
import org.elasticsearch.search.aggregations.AggregatorFactories;
import org.elasticsearch.search.aggregations.AggregatorFactory;
import org.elasticsearch.search.aggregations.bucket.BucketUtils;
import org.elasticsearch.search.aggregations.bucket.significant.heuristics.SignificanceHeuristic;
import org.elasticsearch.search.aggregations.bucket.terms.IncludeExclude;
import org.elasticsearch.search.aggregations.bucket.terms.TermsAggregator;
import org.elasticsearch.search.aggregations.bucket.terms.TermsAggregator.BucketCountThresholds;
import org.elasticsearch.search.aggregations.pipeline.PipelineAggregator;
import org.elasticsearch.search.internal.SearchContext;

import java.io.IOException;
import java.util.List;
import java.util.Map;

public class SignificantTextAggregatorFactory extends AggregatorFactory
        implements Releasable {

    private final IncludeExclude includeExclude;
    private String indexedFieldName;
    private MappedFieldType fieldType;
    private final String[] sourceFieldNames;
    private FilterableTermsEnum termsEnum;
    private int numberOfAggregatorsCreated;
    private final Query filter;
    private final int supersetNumDocs;
    private final TermsAggregator.BucketCountThresholds bucketCountThresholds;
    private final SignificanceHeuristic significanceHeuristic;
    private final DocValueFormat format = DocValueFormat.RAW;
    private final boolean filterDuplicateText;

    public SignificantTextAggregatorFactory(String name, IncludeExclude includeExclude,
            QueryBuilder filterBuilder, TermsAggregator.BucketCountThresholds bucketCountThresholds,
            SignificanceHeuristic significanceHeuristic, SearchContext context, AggregatorFactory parent,
            AggregatorFactories.Builder subFactoriesBuilder, String fieldName, String [] sourceFieldNames,
            boolean filterDuplicateText, Map<String, Object> metaData) throws IOException {
        super(name, context, parent, subFactoriesBuilder, metaData);

        // Note that if the field is unmapped (its field type is null), we don't fail,
        // and just use the given field name as a placeholder.
        this.fieldType = context.getQueryShardContext().fieldMapper(fieldName);
        this.indexedFieldName = fieldType != null ? fieldType.name() : fieldName;
        this.sourceFieldNames = sourceFieldNames == null
            ? new String[] { indexedFieldName }
            : sourceFieldNames;

        this.includeExclude = includeExclude;
        this.filter = filterBuilder == null
                ? null
                : filterBuilder.toQuery(context.getQueryShardContext());
        this.filterDuplicateText = filterDuplicateText;
        IndexSearcher searcher = context.searcher();
        // Important - need to use the doc count that includes deleted docs
        // or we have this issue: https://github.com/elastic/elasticsearch/issues/7951
        this.supersetNumDocs = filter == null
                ? searcher.getIndexReader().maxDoc()
                : searcher.count(filter);
        this.bucketCountThresholds = bucketCountThresholds;
        this.significanceHeuristic = significanceHeuristic;
    }

    /**
     * Get the number of docs in the superset.
     */
    public long getSupersetNumDocs() {
        return supersetNumDocs;
    }

    private FilterableTermsEnum getTermsEnum(String field) throws IOException {
        if (termsEnum != null) {
            return termsEnum;
        }
        IndexReader reader = context.searcher().getIndexReader();
        if (numberOfAggregatorsCreated > 1) {
            termsEnum = new FreqTermsEnum(reader, field, true, false, filter, context.bigArrays());
        } else {
            termsEnum = new FilterableTermsEnum(reader, indexedFieldName, PostingsEnum.NONE, filter);
        }
        return termsEnum;
    }

    private long getBackgroundFrequency(String value) throws IOException {
        Query query = fieldType.termQuery(value, context.getQueryShardContext());
        if (query instanceof TermQuery) {
            // for types that use the inverted index, we prefer using a caching terms
            // enum that will do a better job at reusing index inputs
            Term term = ((TermQuery) query).getTerm();
            FilterableTermsEnum termsEnum = getTermsEnum(term.field());
            if (termsEnum.seekExact(term.bytes())) {
                return termsEnum.docFreq();
            } else {
                return 0;
            }
        }
        // otherwise do it the naive way
        if (filter != null) {
            query = new BooleanQuery.Builder()
                    .add(query, Occur.FILTER)
                    .add(filter, Occur.FILTER)
                    .build();
        }
        return context.searcher().count(query);
    }

    public long getBackgroundFrequency(BytesRef termBytes) throws IOException {
        String value = format.format(termBytes).toString();
        return getBackgroundFrequency(value);
    }


    @Override
    public void close() {
        try {
            if (termsEnum instanceof Releasable) {
                ((Releasable) termsEnum).close();
            }
        } finally {
            termsEnum = null;
        }
    }

    @Override
    protected Aggregator createInternal(Aggregator parent, boolean collectsFromSingleBucket,
            List<PipelineAggregator> pipelineAggregators, Map<String, Object> metaData)
            throws IOException {
        if (collectsFromSingleBucket == false) {
            return asMultiBucketAggregator(this, context, parent);
        }

        numberOfAggregatorsCreated++;
        BucketCountThresholds bucketCountThresholds = new BucketCountThresholds(this.bucketCountThresholds);
        if (bucketCountThresholds.getShardSize() == SignificantTextAggregationBuilder.DEFAULT_BUCKET_COUNT_THRESHOLDS.getShardSize()) {
            // The user has not made a shardSize selection.
            // Use default heuristic to avoid any wrong-ranking caused by
            // distributed counting but request double the usual amount.
            // We typically need more than the number of "top" terms requested
            // by other aggregations as the significance algorithm is in less
            // of a position to down-select at shard-level - some of the things
            // we want to find have only one occurrence on each shard and as
            // such are impossible to differentiate from non-significant terms
            // at that early stage.
            bucketCountThresholds.setShardSize(2 * BucketUtils.suggestShardSideQueueSize(bucketCountThresholds.getRequiredSize()));
        }

//        TODO - need to check with mapping that this is indeed a text field....

        IncludeExclude.StringFilter incExcFilter = includeExclude == null ? null:
            includeExclude.convertToStringFilter(DocValueFormat.RAW);

        return new SignificantTextAggregator(name, factories, context, parent, pipelineAggregators, bucketCountThresholds,
                incExcFilter, significanceHeuristic, this, indexedFieldName, sourceFieldNames, filterDuplicateText, metaData);

    }
}
