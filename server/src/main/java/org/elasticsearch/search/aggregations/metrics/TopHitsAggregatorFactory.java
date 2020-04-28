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

import org.elasticsearch.index.query.QueryShardContext;
import org.elasticsearch.search.aggregations.Aggregator;
import org.elasticsearch.search.aggregations.AggregatorFactories;
import org.elasticsearch.search.aggregations.AggregatorFactory;
import org.elasticsearch.search.fetch.StoredFieldsContext;
import org.elasticsearch.search.fetch.subphase.FetchDocValuesContext;
import org.elasticsearch.search.fetch.subphase.FetchDocValuesContext.FieldAndFormat;
import org.elasticsearch.search.fetch.subphase.FetchSourceContext;
import org.elasticsearch.search.fetch.subphase.ScriptFieldsContext;
import org.elasticsearch.search.fetch.subphase.highlight.HighlightBuilder;
import org.elasticsearch.search.internal.SearchContext;
import org.elasticsearch.search.internal.SubSearchContext;
import org.elasticsearch.search.sort.SortAndFormats;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Optional;

class TopHitsAggregatorFactory extends AggregatorFactory {

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
                                Map<String, Object> metadata) throws IOException {
        super(name, queryShardContext, parent, subFactories, metadata);
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
    }

    @Override
    public Aggregator createInternal(SearchContext searchContext,
                                        Aggregator parent,
                                        boolean collectsFromSingleBucket,
                                        Map<String, Object> metadata) throws IOException {
        SubSearchContext subSearchContext = new SubSearchContext(searchContext);
        subSearchContext.parsedQuery(searchContext.parsedQuery());
        subSearchContext.explain(explain);
        subSearchContext.version(version);
        subSearchContext.seqNoAndPrimaryTerm(seqNoAndPrimaryTerm);
        subSearchContext.trackScores(trackScores);
        subSearchContext.from(from);
        subSearchContext.size(size);
        if (sort.isPresent()) {
            subSearchContext.sort(sort.get());
        }
        if (storedFieldsContext != null) {
            subSearchContext.storedFieldsContext(storedFieldsContext);
        }
        if (docValueFields != null) {
            subSearchContext.docValuesContext(new FetchDocValuesContext(docValueFields));
        }
        for (ScriptFieldsContext.ScriptField field : scriptFields) {
            subSearchContext.scriptFields().add(field);
            }
        if (fetchSourceContext != null) {
            subSearchContext.fetchSourceContext(fetchSourceContext);
        }
        if (highlightBuilder != null) {
            subSearchContext.highlight(highlightBuilder.build(searchContext.getQueryShardContext()));
        }
        return new TopHitsAggregator(searchContext.fetchPhase(), subSearchContext, name, searchContext, parent, metadata);
    }

}
