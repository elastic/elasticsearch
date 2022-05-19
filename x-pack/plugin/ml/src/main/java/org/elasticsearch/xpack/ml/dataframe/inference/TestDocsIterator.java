/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.ml.dataframe.inference;

import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.internal.OriginSettingClient;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.sort.FieldSortBuilder;
import org.elasticsearch.search.sort.SortBuilders;
import org.elasticsearch.search.sort.SortOrder;
import org.elasticsearch.xpack.core.ClientHelper;
import org.elasticsearch.xpack.core.ml.dataframe.DataFrameAnalyticsConfig;
import org.elasticsearch.xpack.ml.dataframe.DestinationIndex;
import org.elasticsearch.xpack.ml.extractor.ExtractedField;
import org.elasticsearch.xpack.ml.extractor.ExtractedFields;
import org.elasticsearch.xpack.ml.utils.persistence.SearchAfterDocumentsIterator;

import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

public class TestDocsIterator extends SearchAfterDocumentsIterator<SearchHit> {

    private final DataFrameAnalyticsConfig config;
    private Long lastDocId;
    private final Map<String, String> docValueFieldAndFormatPairs;

    TestDocsIterator(OriginSettingClient client, DataFrameAnalyticsConfig config, ExtractedFields extractedFields, Long lastIncrementalId) {
        super(client, config.getDest().getIndex(), true);
        this.config = Objects.requireNonNull(config);
        this.docValueFieldAndFormatPairs = buildDocValueFieldAndFormatPairs(extractedFields);
        this.lastDocId = lastIncrementalId;
    }

    private static Map<String, String> buildDocValueFieldAndFormatPairs(ExtractedFields extractedFields) {
        Map<String, String> docValueFieldAndFormatPairs = new HashMap<>();
        for (ExtractedField docValueField : extractedFields.getDocValueFields()) {
            docValueFieldAndFormatPairs.put(docValueField.getSearchField(), docValueField.getDocValueFormat());
        }
        return docValueFieldAndFormatPairs;
    }

    @Override
    protected QueryBuilder getQuery() {
        return QueryBuilders.boolQuery()
            .mustNot(QueryBuilders.termQuery(config.getDest().getResultsField() + "." + DestinationIndex.IS_TRAINING, true));
    }

    @Override
    protected FieldSortBuilder sortField() {
        return SortBuilders.fieldSort(DestinationIndex.INCREMENTAL_ID).order(SortOrder.ASC);
    }

    @Override
    protected SearchHit map(SearchHit hit) {
        return hit;
    }

    @Override
    protected Object[] searchAfterFields() {
        return lastDocId == null ? null : new Object[] { lastDocId };
    }

    @Override
    protected void extractSearchAfterFields(SearchHit lastSearchHit) {
        lastDocId = (long) lastSearchHit.getSortValues()[0];
    }

    @Override
    protected SearchResponse executeSearchRequest(SearchRequest searchRequest) {
        return ClientHelper.executeWithHeaders(
            config.getHeaders(),
            ClientHelper.ML_ORIGIN,
            client(),
            () -> client().search(searchRequest).actionGet()
        );
    }

    @Override
    protected Map<String, String> docValueFieldAndFormatPairs() {
        return docValueFieldAndFormatPairs;
    }
}
