/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.ml.datafeed.persistence;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.ElasticsearchParseException;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.DelegatingActionListener;
import org.elasticsearch.action.DocWriteRequest;
import org.elasticsearch.action.DocWriteResponse;
import org.elasticsearch.action.delete.DeleteRequest;
import org.elasticsearch.action.delete.DeleteResponse;
import org.elasticsearch.action.delete.TransportDeleteAction;
import org.elasticsearch.action.get.GetRequest;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.get.TransportGetAction;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.TransportIndexAction;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.support.IndicesOptions;
import org.elasticsearch.action.support.WriteRequest;
import org.elasticsearch.client.internal.Client;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.regex.Regex;
import org.elasticsearch.common.xcontent.LoggingDeprecationHandler;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.core.Tuple;
import org.elasticsearch.index.IndexNotFoundException;
import org.elasticsearch.index.engine.VersionConflictEngineException;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.TermQueryBuilder;
import org.elasticsearch.index.query.TermsQueryBuilder;
import org.elasticsearch.index.query.WildcardQueryBuilder;
import org.elasticsearch.persistent.PersistentTasksCustomMetadata;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.tasks.TaskId;
import org.elasticsearch.xcontent.NamedXContentRegistry;
import org.elasticsearch.xcontent.ToXContent;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentFactory;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xcontent.XContentType;
import org.elasticsearch.xpack.core.ClientHelper;
import org.elasticsearch.xpack.core.action.util.ExpandedIdsMatcher;
import org.elasticsearch.xpack.core.ml.MlConfigIndex;
import org.elasticsearch.xpack.core.ml.MlTasks;
import org.elasticsearch.xpack.core.ml.datafeed.DatafeedConfig;
import org.elasticsearch.xpack.core.ml.datafeed.DatafeedUpdate;
import org.elasticsearch.xpack.core.ml.job.config.Job;
import org.elasticsearch.xpack.core.ml.utils.ExceptionsHelper;
import org.elasticsearch.xpack.core.ml.utils.MlStrings;
import org.elasticsearch.xpack.core.ml.utils.ToXContentParams;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.SortedSet;
import java.util.TreeSet;
import java.util.function.BiConsumer;

import static org.elasticsearch.xpack.core.ClientHelper.ML_ORIGIN;
import static org.elasticsearch.xpack.core.ClientHelper.executeAsyncWithOrigin;

/**
 * This class implements CRUD operation for the
 * datafeed configuration document
 *
 * The number of datafeeds returned in a search it limited to
 * {@link MlConfigIndex#CONFIG_INDEX_MAX_RESULTS_WINDOW}.
 * In most cases we expect 10s or 100s of datafeeds to be defined and
 * a search for all datafeeds should return all.
 */
public class DatafeedConfigProvider {

    private static final Logger logger = LogManager.getLogger(DatafeedConfigProvider.class);
    private final Client client;
    private final NamedXContentRegistry xContentRegistry;
    private final ClusterService clusterService;

    public static final Map<String, String> TO_XCONTENT_PARAMS = Map.of(ToXContentParams.FOR_INTERNAL_STORAGE, "true");

    public DatafeedConfigProvider(Client client, NamedXContentRegistry xContentRegistry, ClusterService clusterService) {
        this.client = client;
        this.xContentRegistry = xContentRegistry;
        this.clusterService = clusterService;
    }

    /**
     * Persist the datafeed configuration to the config index.
     * It is an error if a datafeed with the same Id already exists -
     * the config will not be overwritten.
     *
     * @param config The datafeed configuration
     * @param listener Listener that returns config augmented with security headers and index response
     */
    public void putDatafeedConfig(
        DatafeedConfig config,
        Map<String, String> headers,
        ActionListener<Tuple<DatafeedConfig, DocWriteResponse>> listener
    ) {

        DatafeedConfig finalConfig;
        if (headers.isEmpty()) {
            finalConfig = config;
        } else {
            // Filter any values in headers that aren't security fields
            finalConfig = new DatafeedConfig.Builder(config).setHeaders(
                ClientHelper.getPersistableSafeSecurityHeaders(headers, clusterService.state())
            ).build();
        }

        final String datafeedId = finalConfig.getId();

        try (XContentBuilder builder = XContentFactory.jsonBuilder()) {
            XContentBuilder source = finalConfig.toXContent(builder, new ToXContent.MapParams(TO_XCONTENT_PARAMS));

            IndexRequest indexRequest = new IndexRequest(MlConfigIndex.indexName()).id(DatafeedConfig.documentId(datafeedId))
                .source(source)
                .opType(DocWriteRequest.OpType.CREATE)
                .setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE);

            executeAsyncWithOrigin(
                client,
                ML_ORIGIN,
                TransportIndexAction.TYPE,
                indexRequest,
                ActionListener.wrap(r -> listener.onResponse(Tuple.tuple(finalConfig, r)), e -> {
                    if (ExceptionsHelper.unwrapCause(e) instanceof VersionConflictEngineException) {
                        // the datafeed already exists
                        listener.onFailure(ExceptionsHelper.datafeedAlreadyExists(datafeedId));
                    } else {
                        listener.onFailure(e);
                    }
                })
            );

        } catch (IOException e) {
            listener.onFailure(new ElasticsearchParseException("Failed to serialise datafeed config with id [" + datafeedId + "]", e));
        }
    }

    /**
     * Get the datafeed config specified by {@code datafeedId}.
     * If the datafeed document is missing a {@code ResourceNotFoundException}
     * is returned via the listener.
     *
     * If the .ml-config index does not exist it is treated as a missing datafeed
     * error.
     *
     * @param datafeedId The datafeed ID
     * @param parentTaskId the parent task ID if available
     * @param datafeedConfigListener The config listener
     */
    public void getDatafeedConfig(
        String datafeedId,
        @Nullable TaskId parentTaskId,
        ActionListener<DatafeedConfig.Builder> datafeedConfigListener
    ) {
        GetRequest getRequest = new GetRequest(MlConfigIndex.indexName(), DatafeedConfig.documentId(datafeedId));
        if (parentTaskId != null) {
            getRequest.setParentTask(parentTaskId);
        }
        executeAsyncWithOrigin(client, ML_ORIGIN, TransportGetAction.TYPE, getRequest, new ActionListener<GetResponse>() {
            @Override
            public void onResponse(GetResponse getResponse) {
                if (getResponse.isExists() == false) {
                    datafeedConfigListener.onFailure(ExceptionsHelper.missingDatafeedException(datafeedId));
                    return;
                }
                BytesReference source = getResponse.getSourceAsBytesRef();
                parseLenientlyFromSource(source, datafeedConfigListener);
            }

            @Override
            public void onFailure(Exception e) {
                if (e.getClass() == IndexNotFoundException.class) {
                    datafeedConfigListener.onFailure(ExceptionsHelper.missingDatafeedException(datafeedId));
                } else {
                    datafeedConfigListener.onFailure(e);
                }
            }
        });
    }

    /**
     * Find any datafeeds that are used by jobs {@code jobIds} i.e. the
     * datafeeds that references any of the jobs in {@code jobIds}.
     *
     * In theory there should never be more than one datafeed referencing a
     * particular job.
     *
     * @param jobIds    The jobs to find the datafeeds of
     * @param listener  Datafeed Id listener
     */
    public void findDatafeedIdsForJobIds(Collection<String> jobIds, ActionListener<Set<String>> listener) {
        SearchSourceBuilder sourceBuilder = new SearchSourceBuilder().query(buildDatafeedJobIdsQuery(jobIds));
        sourceBuilder.fetchSource(false);
        sourceBuilder.size(jobIds.size());
        sourceBuilder.docValueField(DatafeedConfig.ID.getPreferredName(), null);

        SearchRequest searchRequest = client.prepareSearch(MlConfigIndex.indexName())
            .setIndicesOptions(IndicesOptions.lenientExpandOpen())
            .setSource(sourceBuilder)
            .request();

        executeAsyncWithOrigin(
            client.threadPool().getThreadContext(),
            ML_ORIGIN,
            searchRequest,
            listener.<SearchResponse>delegateFailureAndWrap((delegate, response) -> {
                Set<String> datafeedIds = new HashSet<>();
                // There cannot be more than one datafeed per job
                assert response.getHits().getTotalHits().value <= jobIds.size();
                SearchHit[] hits = response.getHits().getHits();

                for (SearchHit hit : hits) {
                    datafeedIds.add(hit.field(DatafeedConfig.ID.getPreferredName()).getValue());
                }

                delegate.onResponse(datafeedIds);
            }),
            client::search
        );
    }

    public void findDatafeedsByJobIds(
        Collection<String> jobIds,
        @Nullable TaskId parentTaskId,
        ActionListener<Map<String, DatafeedConfig.Builder>> listener
    ) {
        SearchRequest searchRequest = client.prepareSearch(MlConfigIndex.indexName())
            .setIndicesOptions(IndicesOptions.lenientExpandOpen())
            .setSource(new SearchSourceBuilder().query(buildDatafeedJobIdsQuery(jobIds)).size(jobIds.size()))
            .request();
        if (parentTaskId != null) {
            searchRequest.setParentTask(parentTaskId);
        }

        executeAsyncWithOrigin(
            client.threadPool().getThreadContext(),
            ML_ORIGIN,
            searchRequest,
            listener.<SearchResponse>delegateFailureAndWrap((delegate, response) -> {
                Map<String, DatafeedConfig.Builder> datafeedsByJobId = new HashMap<>();
                // There cannot be more than one datafeed per job
                assert response.getHits().getTotalHits().value <= jobIds.size();
                SearchHit[] hits = response.getHits().getHits();
                for (SearchHit hit : hits) {
                    DatafeedConfig.Builder builder = parseLenientlyFromSource(hit.getSourceRef());
                    datafeedsByJobId.put(builder.getJobId(), builder);
                }
                delegate.onResponse(datafeedsByJobId);
            }),
            client::search
        );
    }

    /**
     * Delete the datafeed config document
     *
     * @param datafeedId The datafeed id
     * @param actionListener Deleted datafeed listener
     */
    public void deleteDatafeedConfig(String datafeedId, ActionListener<DeleteResponse> actionListener) {
        DeleteRequest request = new DeleteRequest(MlConfigIndex.indexName(), DatafeedConfig.documentId(datafeedId));
        request.setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE);
        executeAsyncWithOrigin(
            client,
            ML_ORIGIN,
            TransportDeleteAction.TYPE,
            request,
            actionListener.delegateFailure((l, deleteResponse) -> {
                if (deleteResponse.getResult() == DocWriteResponse.Result.NOT_FOUND) {
                    l.onFailure(ExceptionsHelper.missingDatafeedException(datafeedId));
                    return;
                }
                assert deleteResponse.getResult() == DocWriteResponse.Result.DELETED;
                l.onResponse(deleteResponse);
            })
        );
    }

    /**
     * Get the datafeed config and apply the {@code update}
     * then index the modified config setting the version in the request.
     *
     * The {@code validator} consumer can be used to perform extra validation
     * but it must call the passed ActionListener. For example a no-op validator
     * would be {@code (updatedConfig, listener) -> listener.onResponse(Boolean.TRUE)}
     *
     * @param datafeedId The Id of the datafeed to update
     * @param update The update
     * @param headers Datafeed headers applied with the update
     * @param validator BiConsumer that accepts the updated config and can perform
     *                  extra validations. {@code validator} must call the passed listener
     * @param updatedConfigListener Updated datafeed config listener
     */
    public void updateDatefeedConfig(
        String datafeedId,
        DatafeedUpdate update,
        Map<String, String> headers,
        BiConsumer<DatafeedConfig, ActionListener<Boolean>> validator,
        ActionListener<DatafeedConfig> updatedConfigListener
    ) {
        GetRequest getRequest = new GetRequest(MlConfigIndex.indexName(), DatafeedConfig.documentId(datafeedId));

        executeAsyncWithOrigin(
            client,
            ML_ORIGIN,
            TransportGetAction.TYPE,
            getRequest,
            new DelegatingActionListener<>(updatedConfigListener) {
                @Override
                public void onResponse(GetResponse getResponse) {
                    if (getResponse.isExists() == false) {
                        delegate.onFailure(ExceptionsHelper.missingDatafeedException(datafeedId));
                        return;
                    }
                    final long seqNo = getResponse.getSeqNo();
                    final long primaryTerm = getResponse.getPrimaryTerm();
                    BytesReference source = getResponse.getSourceAsBytesRef();
                    DatafeedConfig.Builder configBuilder;
                    try {
                        configBuilder = parseLenientlyFromSource(source);
                    } catch (IOException e) {
                        delegate.onFailure(new ElasticsearchParseException("Failed to parse datafeed config [" + datafeedId + "]", e));
                        return;
                    }

                    DatafeedConfig updatedConfig;
                    try {
                        updatedConfig = update.apply(configBuilder.build(), headers, clusterService.state());
                    } catch (Exception e) {
                        delegate.onFailure(e);
                        return;
                    }

                    validator.accept(
                        updatedConfig,
                        delegate.delegateFailureAndWrap(
                            (l, ok) -> indexUpdatedConfig(
                                updatedConfig,
                                seqNo,
                                primaryTerm,
                                l.delegateFailureAndWrap((ll, indexResponse) -> {
                                    assert indexResponse.getResult() == DocWriteResponse.Result.UPDATED;
                                    ll.onResponse(updatedConfig);
                                })
                            )
                        )
                    );
                }
            }
        );
    }

    private void indexUpdatedConfig(DatafeedConfig updatedConfig, long seqNo, long primaryTerm, ActionListener<DocWriteResponse> listener) {
        try (XContentBuilder builder = XContentFactory.jsonBuilder()) {
            XContentBuilder updatedSource = updatedConfig.toXContent(builder, new ToXContent.MapParams(TO_XCONTENT_PARAMS));
            IndexRequest indexRequest = new IndexRequest(MlConfigIndex.indexName()).id(DatafeedConfig.documentId(updatedConfig.getId()))
                .source(updatedSource)
                .setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE);

            indexRequest.setIfSeqNo(seqNo);
            indexRequest.setIfPrimaryTerm(primaryTerm);

            executeAsyncWithOrigin(client, ML_ORIGIN, TransportIndexAction.TYPE, indexRequest, listener);

        } catch (IOException e) {
            listener.onFailure(
                new ElasticsearchParseException("Failed to serialise datafeed config with id [" + updatedConfig.getId() + "]", e)
            );
        }
    }

    /**
     * Expands an expression into the set of matching names. {@code expresssion}
     * may be a wildcard, a datafeed ID or a list of those.
     * If {@code expression} == 'ALL', '*' or the empty string then all
     * datafeed IDs are returned.
     *
     * For example, given a set of names ["foo-1", "foo-2", "bar-1", bar-2"],
     * expressions resolve follows:
     * <ul>
     *     <li>"foo-1" : ["foo-1"]</li>
     *     <li>"bar-1" : ["bar-1"]</li>
     *     <li>"foo-1,foo-2" : ["foo-1", "foo-2"]</li>
     *     <li>"foo-*" : ["foo-1", "foo-2"]</li>
     *     <li>"*-1" : ["bar-1", "foo-1"]</li>
     *     <li>"*" : ["bar-1", "bar-2", "foo-1", "foo-2"]</li>
     *     <li>"_all" : ["bar-1", "bar-2", "foo-1", "foo-2"]</li>
     * </ul>
     *
     * @param expression the expression to resolve
     * @param allowNoMatch if {@code false}, an error is thrown when no name matches the {@code expression}.
     *                     This only applies to wild card expressions, if {@code expression} is not a
     *                     wildcard then setting this true will not suppress the exception
     * @param tasks The current tasks meta-data. For expanding IDs when datafeeds might have missing configurations
     * @param allowMissingConfigs If a datafeed has a task, but is missing a config, allow the ID to be expanded via the existing task
     * @param parentTaskId the parent task ID if available
     * @param listener The expanded datafeed IDs listener
     */
    public void expandDatafeedIds(
        String expression,
        boolean allowNoMatch,
        PersistentTasksCustomMetadata tasks,
        boolean allowMissingConfigs,
        @Nullable TaskId parentTaskId,
        ActionListener<SortedSet<String>> listener
    ) {
        String[] tokens = ExpandedIdsMatcher.tokenizeExpression(expression);
        SearchSourceBuilder sourceBuilder = new SearchSourceBuilder().query(buildDatafeedIdQuery(tokens));
        sourceBuilder.sort(DatafeedConfig.ID.getPreferredName());
        sourceBuilder.fetchSource(false);
        sourceBuilder.docValueField(DatafeedConfig.ID.getPreferredName(), null);

        SearchRequest searchRequest = client.prepareSearch(MlConfigIndex.indexName())
            .setIndicesOptions(IndicesOptions.lenientExpandOpen())
            .setSource(sourceBuilder)
            .setSize(MlConfigIndex.CONFIG_INDEX_MAX_RESULTS_WINDOW)
            .request();
        if (parentTaskId != null) {
            searchRequest.setParentTask(parentTaskId);
        }

        ExpandedIdsMatcher requiredMatches = new ExpandedIdsMatcher(tokens, allowNoMatch);
        Collection<String> matchingStartedDatafeedIds = matchingDatafeedIdsWithTasks(tokens, tasks);

        executeAsyncWithOrigin(
            client.threadPool().getThreadContext(),
            ML_ORIGIN,
            searchRequest,
            listener.<SearchResponse>delegateFailureAndWrap((delegate, response) -> {
                SortedSet<String> datafeedIds = new TreeSet<>();
                SearchHit[] hits = response.getHits().getHits();
                for (SearchHit hit : hits) {
                    datafeedIds.add(hit.field(DatafeedConfig.ID.getPreferredName()).getValue());
                }
                if (allowMissingConfigs) {
                    datafeedIds.addAll(matchingStartedDatafeedIds);
                }

                requiredMatches.filterMatchedIds(datafeedIds);
                if (requiredMatches.hasUnmatchedIds()) {
                    // some required datafeeds were not found
                    delegate.onFailure(ExceptionsHelper.missingDatafeedException(requiredMatches.unmatchedIdsString()));
                    return;
                }

                delegate.onResponse(datafeedIds);
            }),
            client::search
        );

    }

    /**
     * The same logic as {@link #expandDatafeedIds(String, boolean, PersistentTasksCustomMetadata, boolean, TaskId, ActionListener)} but
     * the full datafeed configuration is returned.
     *
     * See {@link #expandDatafeedIds(String, boolean, PersistentTasksCustomMetadata, boolean, TaskId, ActionListener)}
     *
     * @param expression the expression to resolve
     * @param allowNoMatch if {@code false}, an error is thrown when no name matches the {@code expression}.
     *                     This only applies to wild card expressions, if {@code expression} is not a
     *                     wildcard then setting this true will not suppress the exception
     * @param parentTaskId the parent task ID if available
     * @param listener The expanded datafeed config listener
     */
    public void expandDatafeedConfigs(
        String expression,
        boolean allowNoMatch,
        @Nullable TaskId parentTaskId,
        ActionListener<List<DatafeedConfig.Builder>> listener
    ) {
        String[] tokens = ExpandedIdsMatcher.tokenizeExpression(expression);
        SearchSourceBuilder sourceBuilder = new SearchSourceBuilder().query(buildDatafeedIdQuery(tokens));
        sourceBuilder.sort(DatafeedConfig.ID.getPreferredName());

        SearchRequest searchRequest = client.prepareSearch(MlConfigIndex.indexName())
            .setIndicesOptions(IndicesOptions.lenientExpandOpen())
            .setSource(sourceBuilder)
            .setSize(MlConfigIndex.CONFIG_INDEX_MAX_RESULTS_WINDOW)
            .request();
        if (parentTaskId != null) {
            searchRequest.setParentTask(parentTaskId);
        }

        ExpandedIdsMatcher requiredMatches = new ExpandedIdsMatcher(tokens, allowNoMatch);

        executeAsyncWithOrigin(
            client.threadPool().getThreadContext(),
            ML_ORIGIN,
            searchRequest,
            listener.<SearchResponse>delegateFailureAndWrap((delegate, response) -> {
                List<DatafeedConfig.Builder> datafeeds = new ArrayList<>();
                Set<String> datafeedIds = new HashSet<>();
                SearchHit[] hits = response.getHits().getHits();
                for (SearchHit hit : hits) {
                    try {
                        BytesReference source = hit.getSourceRef();
                        DatafeedConfig.Builder datafeed = parseLenientlyFromSource(source);
                        datafeeds.add(datafeed);
                        datafeedIds.add(datafeed.getId());
                    } catch (IOException e) {
                        // TODO A better way to handle this rather than just ignoring the error?
                        logger.error("Error parsing datafeed configuration [" + hit.getId() + "]", e);
                    }
                }

                requiredMatches.filterMatchedIds(datafeedIds);
                if (requiredMatches.hasUnmatchedIds()) {
                    // some required datafeeds were not found
                    delegate.onFailure(ExceptionsHelper.missingDatafeedException(requiredMatches.unmatchedIdsString()));
                    return;
                }

                delegate.onResponse(datafeeds);
            }),
            client::search
        );

    }

    private static QueryBuilder buildDatafeedIdQuery(String[] tokens) {
        QueryBuilder datafeedQuery = new TermQueryBuilder(DatafeedConfig.CONFIG_TYPE.getPreferredName(), DatafeedConfig.TYPE);
        if (Strings.isAllOrWildcard(tokens)) {
            // match all
            return datafeedQuery;
        }

        BoolQueryBuilder boolQueryBuilder = new BoolQueryBuilder();
        boolQueryBuilder.filter(datafeedQuery);
        BoolQueryBuilder shouldQueries = new BoolQueryBuilder();

        List<String> terms = new ArrayList<>();
        for (String token : tokens) {
            if (Regex.isSimpleMatchPattern(token)) {
                shouldQueries.should(new WildcardQueryBuilder(DatafeedConfig.ID.getPreferredName(), token));
            } else {
                terms.add(token);
            }
        }

        if (terms.isEmpty() == false) {
            shouldQueries.should(new TermsQueryBuilder(DatafeedConfig.ID.getPreferredName(), terms));
        }

        if (shouldQueries.should().isEmpty() == false) {
            boolQueryBuilder.filter(shouldQueries);
        }

        return boolQueryBuilder;
    }

    static Collection<String> matchingDatafeedIdsWithTasks(String[] datafeedIdPatterns, PersistentTasksCustomMetadata tasksMetadata) {
        return MlStrings.findMatching(datafeedIdPatterns, MlTasks.startedDatafeedIds(tasksMetadata));
    }

    private static QueryBuilder buildDatafeedJobIdsQuery(Collection<String> jobIds) {
        BoolQueryBuilder boolQueryBuilder = new BoolQueryBuilder();
        boolQueryBuilder.filter(new TermQueryBuilder(DatafeedConfig.CONFIG_TYPE.getPreferredName(), DatafeedConfig.TYPE));
        boolQueryBuilder.filter(new TermsQueryBuilder(Job.ID.getPreferredName(), jobIds));
        return boolQueryBuilder;
    }

    private void parseLenientlyFromSource(BytesReference source, ActionListener<DatafeedConfig.Builder> datafeedConfigListener) {
        try {
            datafeedConfigListener.onResponse(parseLenientlyFromSource(source));
        } catch (Exception e) {
            datafeedConfigListener.onFailure(e);
        }
    }

    private DatafeedConfig.Builder parseLenientlyFromSource(BytesReference source) throws IOException {
        try (
            XContentParser parser = XContentHelper.createParserNotCompressed(
                LoggingDeprecationHandler.XCONTENT_PARSER_CONFIG.withRegistry(xContentRegistry),
                source,
                XContentType.JSON
            )
        ) {
            return DatafeedConfig.LENIENT_PARSER.apply(parser, null);
        }
    }

}
