/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.ml.dataframe.persistence;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.ElasticsearchParseException;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.DocWriteRequest;
import org.elasticsearch.action.index.IndexAction;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.support.IndicesOptions;
import org.elasticsearch.action.support.WriteRequest;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.xcontent.LoggingDeprecationHandler;
import org.elasticsearch.common.xcontent.NamedXContentRegistry;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.index.engine.VersionConflictEngineException;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.xpack.core.action.util.PageParams;
import org.elasticsearch.xpack.core.ml.MlConfigIndex;
import org.elasticsearch.xpack.core.ml.action.GetDataFrameAnalyticsAction;
import org.elasticsearch.xpack.core.ml.dataframe.DataFrameAnalyticsConfig;
import org.elasticsearch.xpack.core.ml.utils.ExceptionsHelper;
import org.elasticsearch.xpack.core.ml.utils.ToXContentParams;

import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;

import static org.elasticsearch.xpack.core.ClientHelper.ML_ORIGIN;
import static org.elasticsearch.xpack.core.ClientHelper.executeAsyncWithOrigin;
import static org.elasticsearch.xpack.core.ClientHelper.filterSecurityHeaders;

public class DataFrameAnalyticsConfigProvider {

    private static final Logger logger = LogManager.getLogger(DataFrameAnalyticsConfigProvider.class);

    private static final int MAX_CONFIGS_SIZE = 10000;

    private static final Map<String, String> TO_XCONTENT_PARAMS = Collections.singletonMap(ToXContentParams.FOR_INTERNAL_STORAGE, "true");

    private final Client client;
    private final NamedXContentRegistry xContentRegistry;

    public DataFrameAnalyticsConfigProvider(Client client, NamedXContentRegistry xContentRegistry) {
        this.client = Objects.requireNonNull(client);
        this.xContentRegistry = xContentRegistry;
    }

    public void put(DataFrameAnalyticsConfig config, Map<String, String> headers, ActionListener<IndexResponse> listener) {
        String id = config.getId();

        if (headers.isEmpty() == false) {
            // Filter any values in headers that aren't security fields
            config = new DataFrameAnalyticsConfig.Builder(config)
                .setHeaders(filterSecurityHeaders(headers))
                .build();
        }
        try (XContentBuilder builder = XContentFactory.jsonBuilder()) {
            config.toXContent(builder, new ToXContent.MapParams(TO_XCONTENT_PARAMS));
            IndexRequest indexRequest = new IndexRequest(MlConfigIndex.indexName())
                    .id(DataFrameAnalyticsConfig.documentId(config.getId()))
                    .opType(DocWriteRequest.OpType.CREATE)
                    .setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE)
                    .source(builder);

            executeAsyncWithOrigin(client, ML_ORIGIN, IndexAction.INSTANCE, indexRequest, ActionListener.wrap(
                listener::onResponse,
                e -> {
                    if (ExceptionsHelper.unwrapCause(e) instanceof VersionConflictEngineException) {
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

    /**
     * Unlike {@link #getMultiple(String, boolean, ActionListener)} this method tries to get the configs that match jobs with tasks.
     * It expects concrete ids and it does not throw if there is no config for a given id.
     */
    public void getConfigsForJobsWithTasksLeniently(Set<String> jobsWithTask, ActionListener<List<DataFrameAnalyticsConfig>> listener) {
        BoolQueryBuilder query = QueryBuilders.boolQuery();
        query.filter(QueryBuilders.termQuery(DataFrameAnalyticsConfig.CONFIG_TYPE.getPreferredName(), DataFrameAnalyticsConfig.TYPE));
        query.filter(QueryBuilders.termsQuery(DataFrameAnalyticsConfig.ID.getPreferredName(), jobsWithTask));

        SearchRequest searchRequest = new SearchRequest(MlConfigIndex.indexName());
        searchRequest.indicesOptions(IndicesOptions.lenientExpandOpen());
        searchRequest.source().size(DataFrameAnalyticsConfigProvider.MAX_CONFIGS_SIZE);
        searchRequest.source().query(query);

        executeAsyncWithOrigin(client.threadPool().getThreadContext(),
            ML_ORIGIN,
            searchRequest,
            new ActionListener<SearchResponse>() {
                @Override
                public void onResponse(SearchResponse searchResponse) {
                    SearchHit[] hits = searchResponse.getHits().getHits();
                    List<DataFrameAnalyticsConfig> configs = new ArrayList<>(hits.length);
                    for (SearchHit hit : hits) {
                        BytesReference sourceBytes = hit.getSourceRef();
                        try (InputStream stream = sourceBytes.streamInput();
                             XContentParser parser = XContentFactory.xContent(XContentType.JSON).createParser(
                                 xContentRegistry, LoggingDeprecationHandler.INSTANCE, stream)) {
                            configs.add(DataFrameAnalyticsConfig.LENIENT_PARSER.apply(parser, null).build());
                        } catch (IOException e) {
                            listener.onFailure(e);
                        }
                    }


                    Set<String> tasksWithoutConfigs = new HashSet<>(jobsWithTask);
                    tasksWithoutConfigs.removeAll(configs.stream().map(DataFrameAnalyticsConfig::getId).collect(Collectors.toList()));
                    if (tasksWithoutConfigs.isEmpty() == false) {
                        logger.warn("Data frame analytics tasks {} have no configs", tasksWithoutConfigs);
                    }
                    listener.onResponse(configs);
                }

                @Override
                public void onFailure(Exception e) {
                    listener.onFailure(e);
                }
            },
            client::search);
    }
}
