/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.ml.dataframe.persistence;

import org.elasticsearch.ElasticsearchParseException;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.DocWriteRequest;
import org.elasticsearch.action.index.IndexAction;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.support.WriteRequest;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.index.engine.VersionConflictEngineException;
import org.elasticsearch.xpack.core.ClientHelper;
import org.elasticsearch.xpack.core.action.util.PageParams;
import org.elasticsearch.xpack.core.ml.action.GetDataFrameAnalyticsAction;
import org.elasticsearch.xpack.core.ml.dataframe.DataFrameAnalyticsConfig;
import org.elasticsearch.xpack.core.ml.job.persistence.AnomalyDetectorsIndex;
import org.elasticsearch.xpack.core.ml.utils.ExceptionsHelper;
import org.elasticsearch.xpack.core.ml.utils.ToXContentParams;

import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;

import static org.elasticsearch.xpack.core.ClientHelper.ML_ORIGIN;
import static org.elasticsearch.xpack.core.ClientHelper.executeAsyncWithOrigin;

public class DataFrameAnalyticsConfigProvider {

    private static final int MAX_CONFIGS_SIZE = 10000;

    private static final Map<String, String> TO_XCONTENT_PARAMS;

    static {
        Map<String, String> modifiable = new HashMap<>();
        modifiable.put(ToXContentParams.INCLUDE_TYPE, "true");
        modifiable.put(ToXContentParams.FOR_INTERNAL_STORAGE, "true");
        TO_XCONTENT_PARAMS = Collections.unmodifiableMap(modifiable);
    }

    private final Client client;

    public DataFrameAnalyticsConfigProvider(Client client) {
        this.client = Objects.requireNonNull(client);
    }

    public void put(DataFrameAnalyticsConfig config, Map<String, String> headers, ActionListener<IndexResponse> listener) {
        String id = config.getId();

        if (headers.isEmpty() == false) {
            // Filter any values in headers that aren't security fields
            DataFrameAnalyticsConfig.Builder builder = new DataFrameAnalyticsConfig.Builder(config);
            Map<String, String> securityHeaders = headers.entrySet().stream()
                .filter(e -> ClientHelper.SECURITY_HEADER_FILTERS.contains(e.getKey()))
                .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
            builder.setHeaders(securityHeaders);
            config = builder.build();
        }
        try (XContentBuilder builder = XContentFactory.jsonBuilder()) {
            config.toXContent(builder, new ToXContent.MapParams(TO_XCONTENT_PARAMS));
            IndexRequest indexRequest = new IndexRequest(AnomalyDetectorsIndex.configIndexName())
                    .id(DataFrameAnalyticsConfig.documentId(config.getId()))
                    .opType(DocWriteRequest.OpType.CREATE)
                    .setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE)
                    .source(builder);

            executeAsyncWithOrigin(client, ML_ORIGIN, IndexAction.INSTANCE, indexRequest, ActionListener.wrap(
                listener::onResponse,
                e -> {
                    if (e instanceof VersionConflictEngineException) {
                        listener.onFailure(ExceptionsHelper.dataFrameAnalyticsAlreadyExists(id));
                    } else {
                        listener.onFailure(e);
                    }
                }
            ));
        } catch (IOException e) {
            listener.onFailure(new ElasticsearchParseException("Failed to serialise data frame analytics with id [" + config.getId()
                + "]"));
        }
    }

    public void get(String id, ActionListener<DataFrameAnalyticsConfig> listener) {
        GetDataFrameAnalyticsAction.Request request = new GetDataFrameAnalyticsAction.Request();
        request.setResourceId(id);
        executeAsyncWithOrigin(client, ML_ORIGIN, GetDataFrameAnalyticsAction.INSTANCE, request, ActionListener.wrap(
            response -> {
                List<DataFrameAnalyticsConfig> analytics = response.getResources().results();
                if (analytics.size() != 1) {
                    listener.onFailure(ExceptionsHelper.badRequestException("Expected a single match for data frame analytics [{}] " +
                        "but got [{}]", id, analytics.size()));
                } else {
                    listener.onResponse(analytics.get(0));
                }
            },
            listener::onFailure
        ));
    }

    /**
     * @param ids a comma separated list of single IDs and/or wildcards
     */
    public void getMultiple(String ids, boolean allowNoMatch, ActionListener<List<DataFrameAnalyticsConfig>> listener) {
        GetDataFrameAnalyticsAction.Request request = new GetDataFrameAnalyticsAction.Request();
        request.setPageParams(new PageParams(0, MAX_CONFIGS_SIZE));
        request.setResourceId(ids);
        request.setAllowNoResources(allowNoMatch);
        executeAsyncWithOrigin(client, ML_ORIGIN, GetDataFrameAnalyticsAction.INSTANCE, request, ActionListener.wrap(
            response -> listener.onResponse(response.getResources().results()), listener::onFailure));
    }
}
