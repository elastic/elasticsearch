/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.search.aggregations.metrics;

import org.elasticsearch.search.aggregations.AggregationExecutionException;
import org.elasticsearch.search.aggregations.Aggregator;
import org.elasticsearch.search.aggregations.AggregatorFactories;
import org.elasticsearch.search.aggregations.AggregatorFactory;
import org.elasticsearch.search.aggregations.CardinalityUpperBound;
import org.elasticsearch.search.aggregations.support.AggregationContext;
import org.elasticsearch.search.fetch.StoredFieldsContext;
import org.elasticsearch.search.fetch.subphase.FetchDocValuesContext;
import org.elasticsearch.search.fetch.subphase.FetchFieldsContext;
import org.elasticsearch.search.fetch.subphase.FetchSourceContext;
import org.elasticsearch.search.fetch.subphase.FieldAndFormat;
import org.elasticsearch.search.fetch.subphase.ScriptFieldsContext;
import org.elasticsearch.search.fetch.subphase.highlight.HighlightBuilder;
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
    private final List<FieldAndFormat> fetchFields;
    private final List<ScriptFieldsContext.ScriptField> scriptFields;
    private final FetchSourceContext fetchSourceContext;

    TopHitsAggregatorFactory(
        String name,
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
        List<FieldAndFormat> fetchFields,
        List<ScriptFieldsContext.ScriptField> scriptFields,
        FetchSourceContext fetchSourceContext,
        AggregationContext context,
        AggregatorFactory parent,
        AggregatorFactories.Builder subFactories,
        Map<String, Object> metadata
    ) throws IOException {
        super(name, context, parent, subFactories, metadata);
        if (context.isInSortOrderExecutionRequired()) {
            throw new AggregationExecutionException("Top hits aggregations cannot be used together with time series aggregations");
        }
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
        this.fetchFields = fetchFields;
        this.scriptFields = scriptFields;
        this.fetchSourceContext = fetchSourceContext;
    }

    @Override
    public Aggregator createInternal(Aggregator parent, CardinalityUpperBound cardinality, Map<String, Object> metadata)
        throws IOException {
        SubSearchContext subSearchContext = context.subSearchContext();
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
            FetchDocValuesContext docValuesContext = new FetchDocValuesContext(
                subSearchContext.getSearchExecutionContext(),
                docValueFields
            );
            subSearchContext.docValuesContext(docValuesContext);
        }
        if (fetchFields != null) {
            FetchFieldsContext fieldsContext = new FetchFieldsContext(fetchFields);
            subSearchContext.fetchFieldsContext(fieldsContext);
        }
        for (ScriptFieldsContext.ScriptField field : scriptFields) {
            subSearchContext.scriptFields().add(field);
        }
        if (fetchSourceContext != null) {
            subSearchContext.fetchSourceContext(fetchSourceContext);
        }
        if (highlightBuilder != null) {
            subSearchContext.highlight(
                highlightBuilder.build(subSearchContext.getSearchExecutionContext(), highlightBuilder.highlightQuery())
            );
        }
        return new TopHitsAggregator(subSearchContext, name, context, parent, metadata);
    }

}
