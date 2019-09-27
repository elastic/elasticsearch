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

package org.elasticsearch.search.aggregations.metrics;

import org.apache.lucene.search.Sort;
import org.elasticsearch.index.query.ParsedQuery;
import org.elasticsearch.index.query.QueryShardContext;
import org.elasticsearch.search.aggregations.Aggregator;
import org.elasticsearch.search.aggregations.AggregatorFactories;
import org.elasticsearch.search.aggregations.AggregatorFactory;
import org.elasticsearch.search.aggregations.pipeline.PipelineAggregator;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.search.fetch.StoredFieldsContext;
import org.elasticsearch.search.fetch.subphase.DocValueFieldsContext;
import org.elasticsearch.search.fetch.subphase.DocValueFieldsContext.FieldAndFormat;
import org.elasticsearch.search.fetch.subphase.FetchSourceContext;
import org.elasticsearch.search.fetch.subphase.ScriptFieldsContext;
import org.elasticsearch.search.fetch.subphase.highlight.HighlightBuilder;
import org.elasticsearch.search.internal.SearchContext;
import org.elasticsearch.search.sort.SortAndFormats;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;

class TopHitsAggregatorFactory extends AggregatorFactory {
    private final QueryShardContext cloneShardContext;
    private final int from;
    private final int size;
    private final boolean explain;
    private final boolean version;
    private final boolean seqNoAndPrimaryTerm;
    private final boolean trackScores;
    private final Optional<SortAndFormats> sort;
    private final HighlightBuilder highlightBuilder;
    private final StoredFieldsContext storedFieldsContext;
    private final List<FieldAndFormat> docValueFields;
    private final List<ScriptFieldsContext.ScriptField> scriptFields;
    private final FetchSourceContext fetchSourceContext;

    TopHitsAggregatorFactory(String name,
                                int from,
                                int size,
                                boolean explain,
                                boolean version,
                                boolean seqNoAndPrimaryTerm,
                                boolean trackScores,
                                Optional<SortAndFormats> sort,
                                HighlightBuilder highlightBuilder,
                                StoredFieldsContext storedFieldsContext,
                                List<FieldAndFormat> docValueFields,
                                List<ScriptFieldsContext.ScriptField> scriptFields,
                                FetchSourceContext fetchSourceContext,
                                QueryShardContext queryShardContext,
                                AggregatorFactory parent,
                                AggregatorFactories.Builder subFactories,
                                Map<String, Object> metaData) throws IOException {
        super(name, queryShardContext, parent, subFactories, metaData);
        this.from = from;
        this.size = size;
        this.explain = explain;
        this.version = version;
        this.seqNoAndPrimaryTerm = seqNoAndPrimaryTerm;
        this.trackScores = trackScores;
        this.sort = sort;
        this.highlightBuilder = highlightBuilder;
        this.storedFieldsContext = storedFieldsContext;
        this.docValueFields = docValueFields;
        this.scriptFields = scriptFields;
        this.fetchSourceContext = fetchSourceContext;
        this.cloneShardContext = new QueryShardContext(queryShardContext);
        if (queryShardContext.nestedScope().getObjectMapper() != null) {
            cloneShardContext.nestedScope().nextLevel(queryShardContext.nestedScope().getObjectMapper());
        }
    }

    @Override
    public Aggregator createInternal(SearchContext searchContext,
                                     Aggregator parent,
                                     boolean collectsFromSingleBucket,
                                     List<PipelineAggregator> pipelineAggregators,
                                     Map<String, Object> metaData) throws IOException {
        SearchContext.Builder builder = new SearchContext.Builder(searchContext.id(),
            searchContext.getTask(),
            searchContext.nodeId(),
            searchContext.indexShard(),
            cloneShardContext,
            searchContext.searcher(),
            searchContext.fetchPhase(),
            searchContext.shardTarget().getClusterAlias(),
            searchContext.numberOfShards(),
            searchContext::getRelativeTimeInMillis,
            new SearchSourceBuilder());
        parse(builder);
        builder.setQuery(new ParsedQuery(searchContext.parsedQuery().query(),
            searchContext.parsedQuery().namedFilters()));
        builder.setExplain(explain);
        builder.setVersion(version);
        builder.setSeqAndPrimaryTerm(seqNoAndPrimaryTerm);
        builder.setTrackScores(trackScores);
        if (from != -1) {
            builder.setFrom(from);
        }
        if (size != -1) {
            builder.setSize(size);
        } else {
            builder.setSize(3);
        }
        if (sort.isPresent()) {
            builder.setSort(sort.get());
        }
        if (storedFieldsContext != null) {
            builder.setStoredFields(storedFieldsContext);
        }
        if (docValueFields != null) {
            builder.setDocValueFields(new DocValueFieldsContext(docValueFields));
        }
        if (scriptFields != null && scriptFields.isEmpty() == false) {
            builder.setScriptFields(new ScriptFieldsContext(scriptFields));
        }
        if (fetchSourceContext != null) {
            builder.setFetchSource(fetchSourceContext);
        }
        if (highlightBuilder != null) {
            builder.buildHighlight(highlightBuilder);
        }
        if (searchContext.rescore().size() > 0
                && (sort.isPresent() == false || Sort.RELEVANCE.equals(sort.get().sort))) {
            // copy the main rescorers if tophits are sorted by relevancy.
            builder.setRescorers(new ArrayList<>(searchContext.rescore()));
        }
        return new TopHitsAggregator(searchContext.fetchPhase(), builder.build(() -> {}), name,
            searchContext, parent, pipelineAggregators, metaData);
    }

    private SearchContext.Builder parse(SearchContext.Builder builder) {

        return builder;
    }

}
