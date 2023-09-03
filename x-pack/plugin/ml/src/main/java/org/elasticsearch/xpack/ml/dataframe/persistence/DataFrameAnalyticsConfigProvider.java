/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.ml.dataframe.persistence;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.ElasticsearchParseException;
import org.elasticsearch.ResourceNotFoundException;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.DelegatingActionListener;
import org.elasticsearch.action.DocWriteRequest;
import org.elasticsearch.action.get.GetAction;
import org.elasticsearch.action.get.GetRequest;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.index.IndexAction;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.support.IndicesOptions;
import org.elasticsearch.action.support.WriteRequest;
import org.elasticsearch.action.support.master.AcknowledgedResponse;
import org.elasticsearch.client.internal.Client;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.xcontent.LoggingDeprecationHandler;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.index.IndexNotFoundException;
import org.elasticsearch.index.engine.VersionConflictEngineException;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.persistent.PersistentTasksCustomMetadata;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.fetch.subphase.FetchSourceContext;
import org.elasticsearch.xcontent.NamedXContentRegistry;
import org.elasticsearch.xcontent.ToXContent;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentFactory;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xcontent.XContentType;
import org.elasticsearch.xpack.core.ClientHelper;
import org.elasticsearch.xpack.core.action.util.PageParams;
import org.elasticsearch.xpack.core.ml.MlConfigIndex;
import org.elasticsearch.xpack.core.ml.MlTasks;
import org.elasticsearch.xpack.core.ml.action.GetDataFrameAnalyticsAction;
import org.elasticsearch.xpack.core.ml.dataframe.DataFrameAnalyticsConfig;
import org.elasticsearch.xpack.core.ml.dataframe.DataFrameAnalyticsConfigUpdate;
import org.elasticsearch.xpack.core.ml.dataframe.DataFrameAnalyticsState;
import org.elasticsearch.xpack.core.ml.job.messages.Messages;
import org.elasticsearch.xpack.core.ml.utils.ExceptionsHelper;
import org.elasticsearch.xpack.core.ml.utils.ToXContentParams;
import org.elasticsearch.xpack.ml.notifications.DataFrameAnalyticsAuditor;

import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

import static org.elasticsearch.xpack.core.ClientHelper.ML_ORIGIN;
import static org.elasticsearch.xpack.core.ClientHelper.executeAsyncWithOrigin;

public class DataFrameAnalyticsConfigProvider {

    private static final Logger logger = LogManager.getLogger(DataFrameAnalyticsConfigProvider.class);

    private static final int MAX_CONFIGS_SIZE = 10000;

    private static final Map<String, String> TO_XCONTENT_PARAMS = Collections.singletonMap(ToXContentParams.FOR_INTERNAL_STORAGE, "true");

    private final Client client;
    private final NamedXContentRegistry xContentRegistry;
    private final DataFrameAnalyticsAuditor auditor;
    private final ClusterService clusterService;

    public DataFrameAnalyticsConfigProvider(
        Client client,
        NamedXContentRegistry xContentRegistry,
        DataFrameAnalyticsAuditor auditor,
        ClusterService clusterService
    ) {
        this.client = Objects.requireNonNull(client);
        this.xContentRegistry = xContentRegistry;
        this.auditor = Objects.requireNonNull(auditor);
        this.clusterService = clusterService;
    }

    /**
     * Puts the given {@link DataFrameAnalyticsConfig} document into the config index.
     */
    public void put(
        final DataFrameAnalyticsConfig config,
        Map<String, String> headers,
        TimeValue timeout,
        ActionListener<DataFrameAnalyticsConfig> listener
    ) {

        ActionListener<AcknowledgedResponse> deleteLeftOverDocsListener = ActionListener.wrap(
            r -> index(prepareConfigForIndex(config, headers), null, listener),
            listener::onFailure
        );

        ActionListener<Boolean> existsListener = ActionListener.wrap(exists -> {
            if (exists) {
                listener.onFailure(ExceptionsHelper.dataFrameAnalyticsAlreadyExists(config.getId()));
            } else {
                deleteLeftOverDocs(config, timeout, deleteLeftOverDocsListener);
            }
        }, listener::onFailure);

        exists(config.getId(), existsListener);
    }

    private DataFrameAnalyticsConfig prepareConfigForIndex(DataFrameAnalyticsConfig config, Map<String, String> headers) {
        return headers.isEmpty()
            ? config
            : new DataFrameAnalyticsConfig.Builder(config).setHeaders(
                ClientHelper.getPersistableSafeSecurityHeaders(headers, clusterService.state())
            ).build();
    }

    private void exists(String jobId, ActionListener<Boolean> listener) {
        ActionListener<GetResponse> getListener = ActionListener.wrap(getResponse -> listener.onResponse(getResponse.isExists()), e -> {
            if (ExceptionsHelper.unwrapCause(e) instanceof IndexNotFoundException) {
                listener.onResponse(false);
            } else {
                listener.onFailure(e);
            }
        });

        GetRequest getRequest = new GetRequest(MlConfigIndex.indexName(), DataFrameAnalyticsConfig.documentId(jobId));
        getRequest.fetchSourceContext(FetchSourceContext.DO_NOT_FETCH_SOURCE);
        executeAsyncWithOrigin(client, ML_ORIGIN, GetAction.INSTANCE, getRequest, getListener);
    }

    private void deleteLeftOverDocs(DataFrameAnalyticsConfig config, TimeValue timeout, ActionListener<AcknowledgedResponse> listener) {
        DataFrameAnalyticsDeleter deleter = new DataFrameAnalyticsDeleter(client, auditor);
        deleter.deleteAllDocuments(config, timeout, ActionListener.wrap(r -> listener.onResponse(r), e -> {
            if (ExceptionsHelper.unwrapCause(e) instanceof ResourceNotFoundException) {
                // This is expected
                listener.onResponse(AcknowledgedResponse.TRUE);
            } else {
                listener.onFailure(ExceptionsHelper.serverError("error deleting prior documents", e));
            }
        }));
    }

    /**
     * Updates the {@link DataFrameAnalyticsConfig} document in the config index using given {@link DataFrameAnalyticsConfigUpdate}.
     */
    public void update(
        DataFrameAnalyticsConfigUpdate update,
        Map<String, String> headers,
        ClusterState clusterState,
        ActionListener<DataFrameAnalyticsConfig> listener
    ) {
        String id = update.getId();

        GetRequest getRequest = new GetRequest(MlConfigIndex.indexName(), DataFrameAnalyticsConfig.documentId(id));
        executeAsyncWithOrigin(client, ML_ORIGIN, GetAction.INSTANCE, getRequest, ActionListener.wrap(getResponse -> {

            // Fail the update request if the config to be updated doesn't exist
            if (getResponse.isExists() == false) {
                listener.onFailure(ExceptionsHelper.missingDataFrameAnalytics(id));
                return;
            }

            // Parse the original config
            DataFrameAnalyticsConfig originalConfig;
            try {
                try (
                    InputStream stream = getResponse.getSourceAsBytesRef().streamInput();
                    XContentParser parser = XContentFactory.xContent(XContentType.JSON)
                        .createParser(xContentRegistry, LoggingDeprecationHandler.INSTANCE, stream)
                ) {
                    originalConfig = DataFrameAnalyticsConfig.LENIENT_PARSER.apply(parser, null).build();
                }
            } catch (IOException e) {
                listener.onFailure(new ElasticsearchParseException("Failed to parse data frame analytics configuration [" + id + "]", e));
                return;
            }

            // Check that the update can be applied given current analytics state
            checkUpdateCanBeApplied(originalConfig, update, clusterState);

            // Merge the original config with the given update object
            DataFrameAnalyticsConfig.Builder updatedConfigBuilder = update.mergeWithConfig(originalConfig);
            if (headers.isEmpty() == false) {
                updatedConfigBuilder.setHeaders(ClientHelper.getPersistableSafeSecurityHeaders(headers, clusterService.state()));
            }
            DataFrameAnalyticsConfig updatedConfig = updatedConfigBuilder.build();

            // Index the update config
            index(updatedConfig, getResponse, ActionListener.wrap(indexedConfig -> {
                auditor.info(id, Messages.getMessage(Messages.DATA_FRAME_ANALYTICS_AUDIT_UPDATED, update.getUpdatedFields()));
                listener.onResponse(indexedConfig);
            }, listener::onFailure));
        }, listener::onFailure));
    }

    private static void checkUpdateCanBeApplied(
        DataFrameAnalyticsConfig originalConfig,
        DataFrameAnalyticsConfigUpdate update,
        ClusterState clusterState
    ) {
        String analyticsId = update.getId();
        PersistentTasksCustomMetadata tasks = clusterState.getMetadata().custom(PersistentTasksCustomMetadata.TYPE);
        DataFrameAnalyticsState analyticsState = MlTasks.getDataFrameAnalyticsState(analyticsId, tasks);
        if (DataFrameAnalyticsState.STOPPED.equals(analyticsState)) {
            // Analytics is stopped, therefore it is safe to proceed with the udpate
            return;
        }
        if (update.requiresRestart(originalConfig)) {
            throw ExceptionsHelper.conflictStatusException(
                Messages.getMessage(Messages.DATA_FRAME_ANALYTICS_CANNOT_UPDATE_IN_CURRENT_STATE, analyticsId, analyticsState)
            );
        }
    }

    /**
     * Indexes the new version of {@link DataFrameAnalyticsConfig} document into the config index.
     *
     * @param config config object to be indexed
     * @param getResponse {@link GetResponse} coming from requesting the previous version of the config.
     *                    If null, this config is indexed for the first time
     * @param listener listener to be called after indexing
     */
    private void index(
        DataFrameAnalyticsConfig config,
        @Nullable GetResponse getResponse,
        ActionListener<DataFrameAnalyticsConfig> listener
    ) {
        try (XContentBuilder builder = XContentFactory.jsonBuilder()) {
            config.toXContent(builder, new ToXContent.MapParams(TO_XCONTENT_PARAMS));
            IndexRequest indexRequest = new IndexRequest(MlConfigIndex.indexName()).id(DataFrameAnalyticsConfig.documentId(config.getId()))
                .setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE)
                .source(builder);
            if (getResponse == null) {
                indexRequest.opType(DocWriteRequest.OpType.CREATE);
            } else {
                indexRequest.opType(DocWriteRequest.OpType.INDEX)
                    .setIfSeqNo(getResponse.getSeqNo())
                    .setIfPrimaryTerm(getResponse.getPrimaryTerm());
            }

            executeAsyncWithOrigin(
                client,
                ML_ORIGIN,
                IndexAction.INSTANCE,
                indexRequest,
                ActionListener.wrap(indexResponse -> listener.onResponse(config), e -> {
                    if (ExceptionsHelper.unwrapCause(e) instanceof VersionConflictEngineException) {
                        listener.onFailure(ExceptionsHelper.dataFrameAnalyticsAlreadyExists(config.getId()));
                    } else {
                        listener.onFailure(e);
                    }
                })
            );
        } catch (IOException e) {
            listener.onFailure(
                new ElasticsearchParseException("Failed to serialise data frame analytics with id [" + config.getId() + "]")
            );
        }
    }

    public void get(String id, ActionListener<DataFrameAnalyticsConfig> listener) {
        GetDataFrameAnalyticsAction.Request request = new GetDataFrameAnalyticsAction.Request();
        request.setResourceId(id);
        executeAsyncWithOrigin(client, ML_ORIGIN, GetDataFrameAnalyticsAction.INSTANCE, request, ActionListener.wrap(response -> {
            List<DataFrameAnalyticsConfig> analytics = response.getResources().results();
            if (analytics.size() != 1) {
                listener.onFailure(
                    ExceptionsHelper.badRequestException(
                        "Expected a single match for data frame analytics [{}] " + "but got [{}]",
                        id,
                        analytics.size()
                    )
                );
            } else {
                listener.onResponse(analytics.get(0));
            }
        }, listener::onFailure));
    }

    /**
     * @param ids a comma separated list of single IDs and/or wildcards
     */
    public void getMultiple(String ids, boolean allowNoMatch, ActionListener<List<DataFrameAnalyticsConfig>> listener) {
        GetDataFrameAnalyticsAction.Request request = new GetDataFrameAnalyticsAction.Request();
        request.setPageParams(new PageParams(0, MAX_CONFIGS_SIZE));
        request.setResourceId(ids);
        request.setAllowNoResources(allowNoMatch);
        executeAsyncWithOrigin(
            client,
            ML_ORIGIN,
            GetDataFrameAnalyticsAction.INSTANCE,
            request,
            ActionListener.wrap(response -> listener.onResponse(response.getResources().results()), listener::onFailure)
        );
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

        executeAsyncWithOrigin(
            client.threadPool().getThreadContext(),
            ML_ORIGIN,
            searchRequest,
            new DelegatingActionListener<SearchResponse, List<DataFrameAnalyticsConfig>>(listener) {
                @Override
                public void onResponse(SearchResponse searchResponse) {
                    SearchHit[] hits = searchResponse.getHits().getHits();
                    List<DataFrameAnalyticsConfig> configs = new ArrayList<>(hits.length);
                    for (SearchHit hit : hits) {
                        BytesReference sourceBytes = hit.getSourceRef();
                        try (
                            InputStream stream = sourceBytes.streamInput();
                            XContentParser parser = XContentFactory.xContent(XContentType.JSON)
                                .createParser(xContentRegistry, LoggingDeprecationHandler.INSTANCE, stream)
                        ) {
                            configs.add(DataFrameAnalyticsConfig.LENIENT_PARSER.apply(parser, null).build());
                        } catch (IOException e) {
                            delegate.onFailure(e);
                        }
                    }

                    Set<String> tasksWithoutConfigs = new HashSet<>(jobsWithTask);
                    configs.stream().map(DataFrameAnalyticsConfig::getId).toList().forEach(tasksWithoutConfigs::remove);
                    if (tasksWithoutConfigs.isEmpty() == false) {
                        logger.warn("Data frame analytics tasks {} have no configs", tasksWithoutConfigs);
                    }
                    delegate.onResponse(configs);
                }
            },
            client::search
        );
    }
}
