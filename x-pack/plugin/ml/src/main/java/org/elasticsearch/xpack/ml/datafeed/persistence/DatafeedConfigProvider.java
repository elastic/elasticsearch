/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.ml.datafeed.persistence;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.ElasticsearchParseException;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.DocWriteRequest;
import org.elasticsearch.action.DocWriteResponse;
import org.elasticsearch.action.delete.DeleteAction;
import org.elasticsearch.action.delete.DeleteRequest;
import org.elasticsearch.action.delete.DeleteResponse;
import org.elasticsearch.action.get.GetAction;
import org.elasticsearch.action.get.GetRequest;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.index.IndexAction;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.support.IndicesOptions;
import org.elasticsearch.action.support.WriteRequest;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.regex.Regex;
import org.elasticsearch.common.xcontent.LoggingDeprecationHandler;
import org.elasticsearch.common.xcontent.NamedXContentRegistry;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.common.xcontent.XContentType;
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
import org.elasticsearch.xpack.core.action.util.ExpandedIdsMatcher;
import org.elasticsearch.xpack.core.ml.MlConfigIndex;
import org.elasticsearch.xpack.core.ml.MlTasks;
import org.elasticsearch.xpack.core.ml.datafeed.DatafeedConfig;
import org.elasticsearch.xpack.core.ml.datafeed.DatafeedUpdate;
import org.elasticsearch.xpack.core.ml.job.config.Job;
import org.elasticsearch.xpack.core.ml.job.persistence.AnomalyDetectorsIndex;
import org.elasticsearch.xpack.core.ml.utils.ExceptionsHelper;
import org.elasticsearch.xpack.core.ml.utils.MlStrings;
import org.elasticsearch.xpack.core.ml.utils.ToXContentParams;

import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.SortedSet;
import java.util.TreeSet;
import java.util.function.BiConsumer;

import static org.elasticsearch.xpack.core.ClientHelper.ML_ORIGIN;
import static org.elasticsearch.xpack.core.ClientHelper.executeAsyncWithOrigin;
import static org.elasticsearch.xpack.core.ClientHelper.filterSecurityHeaders;

/**
 * This class implements CRUD operation for the
 * datafeed configuration document
 *
 * The number of datafeeds returned in a search it limited to
 * {@link AnomalyDetectorsIndex#CONFIG_INDEX_MAX_RESULTS_WINDOW}.
 * In most cases we expect 10s or 100s of datafeeds to be defined and
 * a search for all datafeeds should return all.
 */
public class DatafeedConfigProvider {

    private static final Logger logger = LogManager.getLogger(DatafeedConfigProvider.class);
    private final Client client;
    private final NamedXContentRegistry xContentRegistry;

    public static final Map<String, String> TO_XCONTENT_PARAMS = Map.of(
        ToXContentParams.FOR_INTERNAL_STORAGE, "true");

    public DatafeedConfigProvider(Client client, NamedXContentRegistry xContentRegistry) {
        this.client = client;
        this.xContentRegistry = xContentRegistry;
    }

    /**
     * Persist the datafeed configuration to the config index.
     * It is an error if a datafeed with the same Id already exists -
     * the config will not be overwritten.
     *
     * @param config The datafeed configuration
     * @param listener Index response listener
     */
    public void putDatafeedConfig(DatafeedConfig config, Map<String, String> headers, ActionListener<IndexResponse> listener) {

        if (headers.isEmpty() == false) {
            // Filter any values in headers that aren't security fields
            config = new DatafeedConfig.Builder(config)
                .setHeaders(filterSecurityHeaders(headers))
                .build();
        }

        final String datafeedId = config.getId();

        try (XContentBuilder builder = XContentFactory.jsonBuilder()) {
            XContentBuilder source = config.toXContent(builder, new ToXContent.MapParams(TO_XCONTENT_PARAMS));

            IndexRequest indexRequest = new IndexRequest(MlConfigIndex.indexName())
                    .id(DatafeedConfig.documentId(datafeedId))
                    .source(source)
                    .opType(DocWriteRequest.OpType.CREATE)
                    .setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE);

            executeAsyncWithOrigin(client, ML_ORIGIN, IndexAction.INSTANCE, indexRequest, ActionListener.wrap(
                    listener::onResponse,
                    e -> {
                        if (ExceptionsHelper.unwrapCause(e) instanceof VersionConflictEngineException) {
                            // the dafafeed already exists
                            listener.onFailure(ExceptionsHelper.datafeedAlreadyExists(datafeedId));
                        } else {
                            listener.onFailure(e);
                        }
                    }
            ));

        } catch (IOException e) {
            listener.onFailure(new ElasticsearchParseException("Failed to serialise datafeed config with id [" + config.getId() + "]", e));
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
     * @param datafeedConfigListener The config listener
     */
    public void getDatafeedConfig(String datafeedId, ActionListener<DatafeedConfig.Builder> datafeedConfigListener) {
        GetRequest getRequest = new GetRequest(MlConfigIndex.indexName(), DatafeedConfig.documentId(datafeedId));
        executeAsyncWithOrigin(client, ML_ORIGIN, GetAction.INSTANCE, getRequest, new ActionListener<GetResponse>() {
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
    public void findDatafeedsForJobIds(Collection<String> jobIds, ActionListener<Set<String>> listener) {
        SearchSourceBuilder sourceBuilder = new SearchSourceBuilder().query(buildDatafeedJobIdsQuery(jobIds));
        sourceBuilder.fetchSource(false);
        sourceBuilder.docValueField(DatafeedConfig.ID.getPreferredName(), null);

        SearchRequest searchRequest = client.prepareSearch(MlConfigIndex.indexName())
                .setIndicesOptions(IndicesOptions.lenientExpandOpen())
                .setSize(jobIds.size())
                .setSource(sourceBuilder).request();

        executeAsyncWithOrigin(client.threadPool().getThreadContext(), ML_ORIGIN, searchRequest,
                ActionListener.<SearchResponse>wrap(
                        response -> {
                            Set<String> datafeedIds = new HashSet<>();
                            // There cannot be more than one datafeed per job
                            assert response.getHits().getTotalHits().value <= jobIds.size();
                            SearchHit[] hits = response.getHits().getHits();

                            for (SearchHit hit : hits) {
                                datafeedIds.add(hit.field(DatafeedConfig.ID.getPreferredName()).getValue());
                            }

                            listener.onResponse(datafeedIds);
                        },
                        listener::onFailure)
                , client::search);
    }

    /**
     * Delete the datafeed config document
     *
     * @param datafeedId The datafeed id
     * @param actionListener Deleted datafeed listener
     */
    public void deleteDatafeedConfig(String datafeedId,  ActionListener<DeleteResponse> actionListener) {
        DeleteRequest request = new DeleteRequest(MlConfigIndex.indexName(), DatafeedConfig.documentId(datafeedId));
        request.setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE);
        executeAsyncWithOrigin(client, ML_ORIGIN, DeleteAction.INSTANCE, request, new ActionListener<DeleteResponse>() {
            @Override
            public void onResponse(DeleteResponse deleteResponse) {
                if (deleteResponse.getResult() == DocWriteResponse.Result.NOT_FOUND) {
                    actionListener.onFailure(ExceptionsHelper.missingDatafeedException(datafeedId));
                    return;
                }
                assert deleteResponse.getResult() == DocWriteResponse.Result.DELETED;
                actionListener.onResponse(deleteResponse);
            }
            @Override
            public void onFailure(Exception e) {
                actionListener.onFailure(e);
            }
        });
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
    public void updateDatefeedConfig(String datafeedId, DatafeedUpdate update, Map<String, String> headers,
                                     BiConsumer<DatafeedConfig, ActionListener<Boolean>> validator,
                                     ActionListener<DatafeedConfig> updatedConfigListener) {
        GetRequest getRequest = new GetRequest(MlConfigIndex.indexName(), DatafeedConfig.documentId(datafeedId));

        executeAsyncWithOrigin(client, ML_ORIGIN, GetAction.INSTANCE, getRequest, new ActionListener<GetResponse>() {
            @Override
            public void onResponse(GetResponse getResponse) {
                if (getResponse.isExists() == false) {
                    updatedConfigListener.onFailure(ExceptionsHelper.missingDatafeedException(datafeedId));
                    return;
                }
                final long version = getResponse.getVersion();
                final long seqNo = getResponse.getSeqNo();
                final long primaryTerm = getResponse.getPrimaryTerm();
                BytesReference source = getResponse.getSourceAsBytesRef();
                DatafeedConfig.Builder configBuilder;
                try {
                    configBuilder = parseLenientlyFromSource(source);
                } catch (IOException e) {
                    updatedConfigListener.onFailure(
                            new ElasticsearchParseException("Failed to parse datafeed config [" + datafeedId + "]", e));
                    return;
                }

                DatafeedConfig updatedConfig;
                try {
                    updatedConfig = update.apply(configBuilder.build(), headers);
                } catch (Exception e) {
                    updatedConfigListener.onFailure(e);
                    return;
                }

                ActionListener<Boolean> validatedListener = ActionListener.wrap(
                        ok -> {
                            indexUpdatedConfig(updatedConfig, seqNo, primaryTerm, ActionListener.wrap(
                                    indexResponse -> {
                                        assert indexResponse.getResult() == DocWriteResponse.Result.UPDATED;
                                        updatedConfigListener.onResponse(updatedConfig);
                                    },
                                    updatedConfigListener::onFailure));
                        },
                        updatedConfigListener::onFailure
                );

                validator.accept(updatedConfig, validatedListener);
            }

            @Override
            public void onFailure(Exception e) {
                updatedConfigListener.onFailure(e);
            }
        });
    }

    private void indexUpdatedConfig(DatafeedConfig updatedConfig, long seqNo, long primaryTerm,
                                    ActionListener<IndexResponse> listener) {
        try (XContentBuilder builder = XContentFactory.jsonBuilder()) {
            XContentBuilder updatedSource = updatedConfig.toXContent(builder, new ToXContent.MapParams(TO_XCONTENT_PARAMS));
            IndexRequest indexRequest = new IndexRequest(MlConfigIndex.indexName())
                    .id(DatafeedConfig.documentId(updatedConfig.getId()))
                    .source(updatedSource)
                    .setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE);

            indexRequest.setIfSeqNo(seqNo);
            indexRequest.setIfPrimaryTerm(primaryTerm);

            executeAsyncWithOrigin(client, ML_ORIGIN, IndexAction.INSTANCE, indexRequest, listener);

        } catch (IOException e) {
            listener.onFailure(
                    new ElasticsearchParseException("Failed to serialise datafeed config with id [" + updatedConfig.getId() + "]", e));
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
     * @param allowNoDatafeeds if {@code false}, an error is thrown when no name matches the {@code expression}.
     *                     This only applies to wild card expressions, if {@code expression} is not a
     *                     wildcard then setting this true will not suppress the exception
     * @param tasks The current tasks meta-data. For expanding IDs when datafeeds might have missing configurations
     * @param allowMissingConfigs If a datafeed has a task, but is missing a config, allow the ID to be expanded via the existing task
     * @param listener The expanded datafeed IDs listener
     */
    public void expandDatafeedIds(String expression,
                                  boolean allowNoDatafeeds,
                                  PersistentTasksCustomMetadata tasks,
                                  boolean allowMissingConfigs,
                                  ActionListener<SortedSet<String>> listener) {
        String [] tokens = ExpandedIdsMatcher.tokenizeExpression(expression);
        SearchSourceBuilder sourceBuilder = new SearchSourceBuilder().query(buildDatafeedIdQuery(tokens));
        sourceBuilder.sort(DatafeedConfig.ID.getPreferredName());
        sourceBuilder.fetchSource(false);
        sourceBuilder.docValueField(DatafeedConfig.ID.getPreferredName(), null);

        SearchRequest searchRequest = client.prepareSearch(MlConfigIndex.indexName())
                .setIndicesOptions(IndicesOptions.lenientExpandOpen())
                .setSource(sourceBuilder)
                .setSize(AnomalyDetectorsIndex.CONFIG_INDEX_MAX_RESULTS_WINDOW)
                .request();

        ExpandedIdsMatcher requiredMatches = new ExpandedIdsMatcher(tokens, allowNoDatafeeds);
        Collection<String> matchingStartedDatafeedIds = matchingDatafeedIdsWithTasks(tokens, tasks);

        executeAsyncWithOrigin(client.threadPool().getThreadContext(), ML_ORIGIN, searchRequest,
                ActionListener.<SearchResponse>wrap(
                        response -> {
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
                                listener.onFailure(ExceptionsHelper.missingDatafeedException(requiredMatches.unmatchedIdsString()));
                                return;
                            }

                            listener.onResponse(datafeedIds);
                        },
                        listener::onFailure)
                , client::search);

    }

    /**
     * The same logic as {@link #expandDatafeedIds(String, boolean, PersistentTasksCustomMetadata, boolean, ActionListener)} but
     * the full datafeed configuration is returned.
     *
     * See {@link #expandDatafeedIds(String, boolean, PersistentTasksCustomMetadata, boolean, ActionListener)}
     *
     * @param expression the expression to resolve
     * @param allowNoDatafeeds if {@code false}, an error is thrown when no name matches the {@code expression}.
     *                     This only applies to wild card expressions, if {@code expression} is not a
     *                     wildcard then setting this true will not suppress the exception
     * @param listener The expanded datafeed config listener
     */
    public void expandDatafeedConfigs(String expression, boolean allowNoDatafeeds, ActionListener<List<DatafeedConfig.Builder>> listener) {
        String [] tokens = ExpandedIdsMatcher.tokenizeExpression(expression);
        SearchSourceBuilder sourceBuilder = new SearchSourceBuilder().query(buildDatafeedIdQuery(tokens));
        sourceBuilder.sort(DatafeedConfig.ID.getPreferredName());

        SearchRequest searchRequest = client.prepareSearch(MlConfigIndex.indexName())
                .setIndicesOptions(IndicesOptions.lenientExpandOpen())
                .setSource(sourceBuilder)
                .setSize(AnomalyDetectorsIndex.CONFIG_INDEX_MAX_RESULTS_WINDOW)
                .request();

        ExpandedIdsMatcher requiredMatches = new ExpandedIdsMatcher(tokens, allowNoDatafeeds);

        executeAsyncWithOrigin(client.threadPool().getThreadContext(), ML_ORIGIN, searchRequest,
                ActionListener.<SearchResponse>wrap(
                        response -> {
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
                                listener.onFailure(ExceptionsHelper.missingDatafeedException(requiredMatches.unmatchedIdsString()));
                                return;
                            }

                            listener.onResponse(datafeeds);
                        },
                        listener::onFailure)
                , client::search);

    }

    private QueryBuilder buildDatafeedIdQuery(String [] tokens) {
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

    private QueryBuilder buildDatafeedJobIdsQuery(Collection<String> jobIds) {
        BoolQueryBuilder boolQueryBuilder = new BoolQueryBuilder();
        boolQueryBuilder.filter(new TermQueryBuilder(DatafeedConfig.CONFIG_TYPE.getPreferredName(), DatafeedConfig.TYPE));
        boolQueryBuilder.filter(new TermsQueryBuilder(Job.ID.getPreferredName(), jobIds));
        return boolQueryBuilder;
    }

    private void parseLenientlyFromSource(BytesReference source, ActionListener<DatafeedConfig.Builder> datafeedConfigListener)  {
        try (InputStream stream = source.streamInput();
             XContentParser parser = XContentFactory.xContent(XContentType.JSON)
                     .createParser(xContentRegistry, LoggingDeprecationHandler.INSTANCE, stream)) {
            datafeedConfigListener.onResponse(DatafeedConfig.LENIENT_PARSER.apply(parser, null));
        } catch (Exception e) {
            datafeedConfigListener.onFailure(e);
        }
    }

    private DatafeedConfig.Builder parseLenientlyFromSource(BytesReference source) throws IOException {
        try (InputStream stream = source.streamInput();
             XContentParser parser = XContentFactory.xContent(XContentType.JSON)
                     .createParser(xContentRegistry, LoggingDeprecationHandler.INSTANCE, stream)) {
            return DatafeedConfig.LENIENT_PARSER.apply(parser, null);
        }
    }
}
