/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.transform;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.ResourceNotFoundException;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.search.SearchRequestBuilder;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.license.XPackLicenseState;
import org.elasticsearch.search.aggregations.Aggregation;
import org.elasticsearch.search.aggregations.AggregationBuilders;
import org.elasticsearch.search.aggregations.metrics.NumericMetricsAggregation;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.xpack.core.ClientHelper;
import org.elasticsearch.xpack.core.XPackField;
import org.elasticsearch.xpack.core.action.XPackInfoFeatureAction;
import org.elasticsearch.xpack.core.action.XPackInfoFeatureTransportAction;
import org.elasticsearch.xpack.core.transform.TransformField;
import org.elasticsearch.xpack.core.transform.transforms.TransformIndexerStats;
import org.elasticsearch.xpack.core.transform.transforms.TransformStoredDoc;
import org.elasticsearch.xpack.core.transform.transforms.persistence.TransformInternalIndexConstants;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class TransformInfoTransportAction extends XPackInfoFeatureTransportAction {

    private final XPackLicenseState licenseState;

    private static final Logger logger = LogManager.getLogger(TransformInfoTransportAction.class);

    public static final String[] PROVIDED_STATS = new String[] {
        TransformIndexerStats.NUM_PAGES.getPreferredName(),
        TransformIndexerStats.NUM_INPUT_DOCUMENTS.getPreferredName(),
        TransformIndexerStats.NUM_OUTPUT_DOCUMENTS.getPreferredName(),
        TransformIndexerStats.NUM_INVOCATIONS.getPreferredName(),
        TransformIndexerStats.INDEX_TIME_IN_MS.getPreferredName(),
        TransformIndexerStats.SEARCH_TIME_IN_MS.getPreferredName(),
        TransformIndexerStats.PROCESSING_TIME_IN_MS.getPreferredName(),
        TransformIndexerStats.INDEX_TOTAL.getPreferredName(),
        TransformIndexerStats.SEARCH_TOTAL.getPreferredName(),
        TransformIndexerStats.PROCESSING_TOTAL.getPreferredName(),
        TransformIndexerStats.INDEX_FAILURES.getPreferredName(),
        TransformIndexerStats.SEARCH_FAILURES.getPreferredName(),
        TransformIndexerStats.EXPONENTIAL_AVG_CHECKPOINT_DURATION_MS.getPreferredName(),
        TransformIndexerStats.EXPONENTIAL_AVG_DOCUMENTS_INDEXED.getPreferredName(),
        TransformIndexerStats.EXPONENTIAL_AVG_DOCUMENTS_PROCESSED.getPreferredName(), };

    @Inject
    public TransformInfoTransportAction(
        TransportService transportService,
        ActionFilters actionFilters,
        XPackLicenseState licenseState
    ) {
        super(XPackInfoFeatureAction.TRANSFORM.name(), transportService, actionFilters);
        this.licenseState = licenseState;
    }

    @Override
    public String name() {
        return XPackField.TRANSFORM;
    }

    @Override
    public boolean available() {
        return licenseState.isAllowed(XPackLicenseState.Feature.TRANSFORM);
    }

    @Override
    public boolean enabled() {
        return true;
    }

    static TransformIndexerStats parseSearchAggs(SearchResponse searchResponse) {
        List<Double> statisticsList = new ArrayList<>(PROVIDED_STATS.length);

        for (String statName : PROVIDED_STATS) {
            Aggregation agg = searchResponse.getAggregations().get(statName);

            if (agg instanceof NumericMetricsAggregation.SingleValue) {
                statisticsList.add(((NumericMetricsAggregation.SingleValue) agg).value());
            } else {
                statisticsList.add(0.0);
            }
        }
        return new TransformIndexerStats(
            statisticsList.get(0).longValue(),  // numPages
            statisticsList.get(1).longValue(),  // numInputDocuments
            statisticsList.get(2).longValue(),  // numOutputDocuments
            statisticsList.get(3).longValue(),  // numInvocations
            statisticsList.get(4).longValue(),  // indexTime
            statisticsList.get(5).longValue(),  // searchTime
            statisticsList.get(6).longValue(),  // processingTime
            statisticsList.get(7).longValue(),  // indexTotal
            statisticsList.get(8).longValue(),  // searchTotal
            statisticsList.get(9).longValue(),  // processingTotal
            statisticsList.get(10).longValue(),  // indexFailures
            statisticsList.get(11).longValue(), // searchFailures
            statisticsList.get(12), // exponential_avg_checkpoint_duration_ms
            statisticsList.get(13), // exponential_avg_documents_indexed
            statisticsList.get(14)  // exponential_avg_documents_processed
        );
    }

    static void getStatisticSummations(Client client, ActionListener<TransformIndexerStats> statsListener) {
        QueryBuilder queryBuilder = QueryBuilders.constantScoreQuery(
            QueryBuilders.boolQuery()
                .filter(QueryBuilders.termQuery(TransformField.INDEX_DOC_TYPE.getPreferredName(), TransformStoredDoc.NAME))
        );

        SearchRequestBuilder requestBuilder = client.prepareSearch(
            TransformInternalIndexConstants.INDEX_NAME_PATTERN,
            TransformInternalIndexConstants.INDEX_NAME_PATTERN_DEPRECATED
        ).setSize(0).setQuery(queryBuilder);

        final String path = TransformField.STATS_FIELD.getPreferredName() + ".";
        for (String statName : PROVIDED_STATS) {
            requestBuilder.addAggregation(AggregationBuilders.sum(statName).field(path + statName));
        }

        ActionListener<SearchResponse> getStatisticSummationsListener = ActionListener.wrap(searchResponse -> {
            if (searchResponse.getShardFailures().length > 0) {
                logger.error(
                    "statistics summations search returned shard failures: {}",
                    Arrays.toString(searchResponse.getShardFailures())
                );
            }

            statsListener.onResponse(parseSearchAggs(searchResponse));
        }, failure -> {
            if (failure instanceof ResourceNotFoundException) {
                statsListener.onResponse(new TransformIndexerStats());
            } else {
                statsListener.onFailure(failure);
            }
        });
        ClientHelper.executeAsyncWithOrigin(
            client.threadPool().getThreadContext(),
            ClientHelper.TRANSFORM_ORIGIN,
            requestBuilder.request(),
            getStatisticSummationsListener,
            client::search
        );
    }
}
