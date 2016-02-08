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

package org.elasticsearch.search.aggregations.metrics.tophits;

import org.elasticsearch.common.ParsingException;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.common.xcontent.XContentLocation;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.script.ScriptContext;
import org.elasticsearch.script.SearchScript;
import org.elasticsearch.search.aggregations.Aggregator;
import org.elasticsearch.search.aggregations.AggregatorFactories;
import org.elasticsearch.search.aggregations.AggregatorFactory;
import org.elasticsearch.search.aggregations.InternalAggregation.Type;
import org.elasticsearch.search.aggregations.pipeline.PipelineAggregator;
import org.elasticsearch.search.aggregations.support.AggregationContext;
import org.elasticsearch.search.builder.SearchSourceBuilder.ScriptField;
import org.elasticsearch.search.fetch.fielddata.FieldDataFieldsContext;
import org.elasticsearch.search.fetch.fielddata.FieldDataFieldsFetchSubPhase;
import org.elasticsearch.search.fetch.fielddata.FieldDataFieldsContext.FieldDataField;
import org.elasticsearch.search.fetch.source.FetchSourceContext;
import org.elasticsearch.search.highlight.HighlightBuilder;
import org.elasticsearch.search.internal.SubSearchContext;
import org.elasticsearch.search.sort.SortParseElement;

import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.Map;

public class TopHitsAggregatorFactory extends AggregatorFactory<TopHitsAggregatorFactory> {

    private static final SortParseElement sortParseElement = new SortParseElement();
    private final int from;
    private final int size;
    private final boolean explain;
    private final boolean version;
    private final boolean trackScores;
    private final List<BytesReference> sorts;
    private final HighlightBuilder highlightBuilder;
    private final List<String> fieldNames;
    private final List<String> fieldDataFields;
    private final List<ScriptField> scriptFields;
    private final FetchSourceContext fetchSourceContext;

    public TopHitsAggregatorFactory(String name, Type type, int from, int size, boolean explain, boolean version, boolean trackScores,
            List<BytesReference> sorts, HighlightBuilder highlightBuilder, List<String> fieldNames, List<String> fieldDataFields,
            List<ScriptField> scriptFields, FetchSourceContext fetchSourceContext, AggregationContext context, AggregatorFactory<?> parent,
            AggregatorFactories.Builder subFactories, Map<String, Object> metaData) throws IOException {
        super(name, type, context, parent, subFactories, metaData);
        this.from = from;
        this.size = size;
        this.explain = explain;
        this.version = version;
        this.trackScores = trackScores;
        this.sorts = sorts;
        this.highlightBuilder = highlightBuilder;
        this.fieldNames = fieldNames;
        this.fieldDataFields = fieldDataFields;
        this.scriptFields = scriptFields;
        this.fetchSourceContext = fetchSourceContext;
    }

    @Override
    public Aggregator createInternal(AggregationContext aggregationContext, Aggregator parent, boolean collectsFromSingleBucket,
            List<PipelineAggregator> pipelineAggregators, Map<String, Object> metaData) throws IOException {
        SubSearchContext subSearchContext = new SubSearchContext(aggregationContext.searchContext());
        subSearchContext.explain(explain);
        subSearchContext.version(version);
        subSearchContext.trackScores(trackScores);
        subSearchContext.from(from);
        subSearchContext.size(size);
        if (sorts != null) {
            XContentParser completeSortParser = null;
            try {
                XContentBuilder completeSortBuilder = XContentFactory.jsonBuilder();
                completeSortBuilder.startObject();
                completeSortBuilder.startArray("sort");
                for (BytesReference sort : sorts) {
                    XContentParser parser = XContentFactory.xContent(sort).createParser(sort);
                    parser.nextToken();
                    completeSortBuilder.copyCurrentStructure(parser);
                }
                completeSortBuilder.endArray();
                completeSortBuilder.endObject();
                BytesReference completeSortBytes = completeSortBuilder.bytes();
                completeSortParser = XContentFactory.xContent(completeSortBytes).createParser(completeSortBytes);
                completeSortParser.nextToken();
                completeSortParser.nextToken();
                completeSortParser.nextToken();
                sortParseElement.parse(completeSortParser, subSearchContext);
            } catch (Exception e) {
                XContentLocation location = completeSortParser != null ? completeSortParser.getTokenLocation() : null;
                throw new ParsingException(location, "failed to parse sort source in aggregation [" + name + "]", e);
            }
        }
        if (fieldNames != null) {
            subSearchContext.fieldNames().addAll(fieldNames);
        }
        if (fieldDataFields != null) {
            FieldDataFieldsContext fieldDataFieldsContext = subSearchContext
                    .getFetchSubPhaseContext(FieldDataFieldsFetchSubPhase.CONTEXT_FACTORY);
            for (String field : fieldDataFields) {
                fieldDataFieldsContext.add(new FieldDataField(field));
            }
            fieldDataFieldsContext.setHitExecutionNeeded(true);
        }
        if (scriptFields != null) {
            for (ScriptField field : scriptFields) {
                SearchScript searchScript = subSearchContext.scriptService().search(subSearchContext.lookup(), field.script(),
                        ScriptContext.Standard.SEARCH, Collections.emptyMap());
                subSearchContext.scriptFields().add(new org.elasticsearch.search.fetch.script.ScriptFieldsContext.ScriptField(
                        field.fieldName(), searchScript, field.ignoreFailure()));
            }
        }
        if (fetchSourceContext != null) {
            subSearchContext.fetchSourceContext(fetchSourceContext);
        }
        if (highlightBuilder != null) {
            subSearchContext.highlight(highlightBuilder.build(aggregationContext.searchContext().indexShard().getQueryShardContext()));
        }
        return new TopHitsAggregator(aggregationContext.searchContext().fetchPhase(), subSearchContext, name, aggregationContext, parent,
                pipelineAggregators, metaData);
    }

}
