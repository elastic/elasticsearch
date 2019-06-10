/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.dataframe.persistence;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.ElasticsearchParseException;
import org.elasticsearch.ResourceAlreadyExistsException;
import org.elasticsearch.ResourceNotFoundException;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.DocWriteRequest;
import org.elasticsearch.action.get.GetAction;
import org.elasticsearch.action.get.GetRequest;
import org.elasticsearch.action.index.IndexAction;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
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
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.index.reindex.DeleteByQueryAction;
import org.elasticsearch.index.reindex.DeleteByQueryRequest;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.sort.SortOrder;
import org.elasticsearch.xpack.core.action.util.ExpandedIdsMatcher;
import org.elasticsearch.xpack.core.action.util.PageParams;
import org.elasticsearch.xpack.core.dataframe.DataFrameField;
import org.elasticsearch.xpack.core.dataframe.DataFrameMessages;
import org.elasticsearch.xpack.core.dataframe.transforms.DataFrameTransformCheckpoint;
import org.elasticsearch.xpack.core.dataframe.transforms.DataFrameTransformConfig;
import org.elasticsearch.xpack.core.dataframe.transforms.DataFrameTransformStateAndStats;

import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import static org.elasticsearch.xpack.core.ClientHelper.DATA_FRAME_ORIGIN;
import static org.elasticsearch.xpack.core.ClientHelper.executeAsyncWithOrigin;

public class DataFrameTransformsConfigManager {

    private static final Logger logger = LogManager.getLogger(DataFrameTransformsConfigManager.class);

    public static final Map<String, String> TO_XCONTENT_PARAMS = Collections.singletonMap(DataFrameField.FOR_INTERNAL_STORAGE, "true");

    private final Client client;
    private final NamedXContentRegistry xContentRegistry;

    public DataFrameTransformsConfigManager(Client client, NamedXContentRegistry xContentRegistry) {
        this.client = client;
        this.xContentRegistry = xContentRegistry;
    }

    /**
     * Persist a checkpoint in the internal index
     *
     * @param checkpoint the @link{DataFrameTransformCheckpoint}
     * @param listener listener to call after request has been made
     */
    public void putTransformCheckpoint(DataFrameTransformCheckpoint checkpoint, ActionListener<Boolean> listener) {
        try (XContentBuilder builder = XContentFactory.jsonBuilder()) {
            XContentBuilder source = checkpoint.toXContent(builder, new ToXContent.MapParams(TO_XCONTENT_PARAMS));

            IndexRequest indexRequest = new IndexRequest(DataFrameInternalIndex.INDEX_NAME)
                    .opType(DocWriteRequest.OpType.INDEX)
                    .setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE)
                    .id(DataFrameTransformCheckpoint.documentId(checkpoint.getTransformId(), checkpoint.getCheckpoint()))
                    .source(source);

            executeAsyncWithOrigin(client, DATA_FRAME_ORIGIN, IndexAction.INSTANCE, indexRequest, ActionListener.wrap(r -> {
                listener.onResponse(true);
            }, listener::onFailure));
        } catch (IOException e) {
            // not expected to happen but for the sake of completeness
            listener.onFailure(e);
        }
    }

    /**
     * Store the transform configuration in the internal index
     *
     * @param transformConfig the @link{DataFrameTransformConfig}
     * @param listener listener to call after request
     */
    public void putTransformConfiguration(DataFrameTransformConfig transformConfig, ActionListener<Boolean> listener) {
        try (XContentBuilder builder = XContentFactory.jsonBuilder()) {
            XContentBuilder source = transformConfig.toXContent(builder, new ToXContent.MapParams(TO_XCONTENT_PARAMS));

            IndexRequest indexRequest = new IndexRequest(DataFrameInternalIndex.INDEX_NAME)
                    .opType(DocWriteRequest.OpType.CREATE)
                    .setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE)
                    .id(DataFrameTransformConfig.documentId(transformConfig.getId()))
                    .source(source);

            executeAsyncWithOrigin(client, DATA_FRAME_ORIGIN, IndexAction.INSTANCE, indexRequest, ActionListener.wrap(r -> {
                listener.onResponse(true);
            }, e -> {
                if (e instanceof VersionConflictEngineException) {
                    // the transform already exists
                    listener.onFailure(new ResourceAlreadyExistsException(
                            DataFrameMessages.getMessage(DataFrameMessages.REST_PUT_DATA_FRAME_TRANSFORM_EXISTS,
                                    transformConfig.getId())));
                } else {
                    listener.onFailure(
                            new RuntimeException(DataFrameMessages.REST_PUT_DATA_FRAME_FAILED_PERSIST_TRANSFORM_CONFIGURATION, e));
                }
            }));
        } catch (IOException e) {
            // not expected to happen but for the sake of completeness
            listener.onFailure(new ElasticsearchParseException(
                    DataFrameMessages.getMessage(DataFrameMessages.REST_DATA_FRAME_FAILED_TO_SERIALIZE_TRANSFORM, transformConfig.getId()),
                    e));
        }
    }

    /**
     * Get a stored checkpoint, requires the transform id as well as the checkpoint id
     *
     * @param transformId the transform id
     * @param checkpoint the checkpoint
     * @param resultListener listener to call after request has been made
     */
    public void getTransformCheckpoint(String transformId, long checkpoint, ActionListener<DataFrameTransformCheckpoint> resultListener) {
        GetRequest getRequest = new GetRequest(DataFrameInternalIndex.INDEX_NAME,
                DataFrameTransformCheckpoint.documentId(transformId, checkpoint));
        executeAsyncWithOrigin(client, DATA_FRAME_ORIGIN, GetAction.INSTANCE, getRequest, ActionListener.wrap(getResponse -> {

            if (getResponse.isExists() == false) {
                // do not fail if checkpoint does not exist but return an empty checkpoint
                logger.trace("found no checkpoint for transform [" + transformId + "], returning empty checkpoint");
                resultListener.onResponse(DataFrameTransformCheckpoint.EMPTY);
                return;
            }
            BytesReference source = getResponse.getSourceAsBytesRef();
            parseCheckpointsLenientlyFromSource(source, transformId, resultListener);
        }, resultListener::onFailure));
    }

    /**
     * Get the transform configuration for a given transform id. This function is only for internal use. For transforms returned via GET
     * data_frame/transforms, see the TransportGetDataFrameTransformsAction
     *
     * @param transformId the transform id
     * @param resultListener listener to call after inner request has returned
     */
    public void getTransformConfiguration(String transformId, ActionListener<DataFrameTransformConfig> resultListener) {
        GetRequest getRequest = new GetRequest(DataFrameInternalIndex.INDEX_NAME, DataFrameTransformConfig.documentId(transformId));
        executeAsyncWithOrigin(client, DATA_FRAME_ORIGIN, GetAction.INSTANCE, getRequest, ActionListener.wrap(getResponse -> {

            if (getResponse.isExists() == false) {
                resultListener.onFailure(new ResourceNotFoundException(
                        DataFrameMessages.getMessage(DataFrameMessages.REST_DATA_FRAME_UNKNOWN_TRANSFORM, transformId)));
                return;
            }
            BytesReference source = getResponse.getSourceAsBytesRef();
            parseTransformLenientlyFromSource(source, transformId, resultListener);
        }, e -> {
            if (e.getClass() == IndexNotFoundException.class) {
                resultListener.onFailure(new ResourceNotFoundException(
                        DataFrameMessages.getMessage(DataFrameMessages.REST_DATA_FRAME_UNKNOWN_TRANSFORM, transformId)));
            } else {
                resultListener.onFailure(e);
            }
        }));
    }

    /**
     * Given some expression comma delimited string of id expressions,
     *   this queries our internal index for the transform Ids that match the expression.
     *
     * The results are sorted in ascending order
     *
     * @param transformIdsExpression The id expression. Can be _all, *, or comma delimited list of simple regex strings
     * @param pageParams             The paging params
     * @param foundIdsListener       The listener on signal on success or failure
     */
    public void expandTransformIds(String transformIdsExpression, PageParams pageParams, ActionListener<List<String>> foundIdsListener) {
        String[] idTokens = ExpandedIdsMatcher.tokenizeExpression(transformIdsExpression);
        QueryBuilder queryBuilder = buildQueryFromTokenizedIds(idTokens, DataFrameTransformConfig.NAME);

        SearchRequest request = client.prepareSearch(DataFrameInternalIndex.INDEX_NAME)
            .addSort(DataFrameField.ID.getPreferredName(), SortOrder.ASC)
            .setFrom(pageParams.getFrom())
            .setSize(pageParams.getSize())
            .setQuery(queryBuilder)
            // We only care about the `id` field, small optimization
            .setFetchSource(DataFrameField.ID.getPreferredName(), "")
            .request();

        final ExpandedIdsMatcher requiredMatches = new ExpandedIdsMatcher(idTokens, true);

        executeAsyncWithOrigin(client.threadPool().getThreadContext(), DATA_FRAME_ORIGIN, request,
            ActionListener.<SearchResponse>wrap(
                searchResponse -> {
                    List<String> ids = new ArrayList<>(searchResponse.getHits().getHits().length);
                    for (SearchHit hit : searchResponse.getHits().getHits()) {
                        BytesReference source = hit.getSourceRef();
                        try (InputStream stream = source.streamInput();
                             XContentParser parser = XContentFactory.xContent(XContentType.JSON).createParser(NamedXContentRegistry.EMPTY,
                                 LoggingDeprecationHandler.INSTANCE, stream)) {
                            ids.add((String) parser.map().get(DataFrameField.ID.getPreferredName()));
                        } catch (IOException e) {
                            foundIdsListener.onFailure(new ElasticsearchParseException("failed to parse search hit for ids", e));
                            return;
                        }
                    }
                    requiredMatches.filterMatchedIds(ids);
                    if (requiredMatches.hasUnmatchedIds()) {
                        // some required Ids were not found
                        foundIdsListener.onFailure(
                            new ResourceNotFoundException(
                                DataFrameMessages.getMessage(DataFrameMessages.REST_DATA_FRAME_UNKNOWN_TRANSFORM,
                                    requiredMatches.unmatchedIdsString())));
                        return;
                    }
                    foundIdsListener.onResponse(ids);
                },
                foundIdsListener::onFailure
            ), client::search);
    }

    /**
     * This deletes the configuration and all other documents corresponding to the transform id (e.g. checkpoints).
     *
     * @param transformId the transform id
     * @param listener listener to call after inner request returned
     */
    public void deleteTransform(String transformId, ActionListener<Boolean> listener) {
        DeleteByQueryRequest request = new DeleteByQueryRequest()
                .setAbortOnVersionConflict(false); //since these documents are not updated, a conflict just means it was deleted previously

        request.indices(DataFrameInternalIndex.INDEX_NAME);
        QueryBuilder query = QueryBuilders.termQuery(DataFrameField.ID.getPreferredName(), transformId);
        request.setQuery(query);
        request.setRefresh(true);

        executeAsyncWithOrigin(client, DATA_FRAME_ORIGIN, DeleteByQueryAction.INSTANCE, request, ActionListener.wrap(deleteResponse -> {

            if (deleteResponse.getDeleted() == 0) {
                listener.onFailure(new ResourceNotFoundException(
                        DataFrameMessages.getMessage(DataFrameMessages.REST_DATA_FRAME_UNKNOWN_TRANSFORM, transformId)));
                return;
            }
            listener.onResponse(true);
        }, e -> {
            if (e.getClass() == IndexNotFoundException.class) {
                listener.onFailure(new ResourceNotFoundException(
                        DataFrameMessages.getMessage(DataFrameMessages.REST_DATA_FRAME_UNKNOWN_TRANSFORM, transformId)));
            } else {
                listener.onFailure(e);
            }
        }));
    }

    public void putOrUpdateTransformStats(DataFrameTransformStateAndStats stats, ActionListener<Boolean> listener) {
        try (XContentBuilder builder = XContentFactory.jsonBuilder()) {
            XContentBuilder source = stats.toXContent(builder, new ToXContent.MapParams(TO_XCONTENT_PARAMS));

            IndexRequest indexRequest = new IndexRequest(DataFrameInternalIndex.INDEX_NAME)
                .setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE)
                .id(DataFrameTransformStateAndStats.documentId(stats.getTransformId()))
                .source(source);

            executeAsyncWithOrigin(client, DATA_FRAME_ORIGIN, IndexAction.INSTANCE, indexRequest, ActionListener.wrap(
                r -> listener.onResponse(true),
                e -> listener.onFailure(new RuntimeException(
                    DataFrameMessages.getMessage(DataFrameMessages.DATA_FRAME_FAILED_TO_PERSIST_STATS, stats.getTransformId()),
                    e))
            ));
        } catch (IOException e) {
            // not expected to happen but for the sake of completeness
            listener.onFailure(new ElasticsearchParseException(
                DataFrameMessages.getMessage(DataFrameMessages.DATA_FRAME_FAILED_TO_PERSIST_STATS, stats.getTransformId()),
                e));
        }
    }

    public void getTransformStats(String transformId, ActionListener<DataFrameTransformStateAndStats> resultListener) {
        GetRequest getRequest = new GetRequest(DataFrameInternalIndex.INDEX_NAME, DataFrameTransformStateAndStats.documentId(transformId));
        executeAsyncWithOrigin(client, DATA_FRAME_ORIGIN, GetAction.INSTANCE, getRequest, ActionListener.wrap(getResponse -> {

            if (getResponse.isExists() == false) {
                resultListener.onFailure(new ResourceNotFoundException(
                    DataFrameMessages.getMessage(DataFrameMessages.DATA_FRAME_UNKNOWN_TRANSFORM_STATS, transformId)));
                return;
            }
            BytesReference source = getResponse.getSourceAsBytesRef();
            try (InputStream stream = source.streamInput();
                 XContentParser parser = XContentFactory.xContent(XContentType.JSON)
                     .createParser(xContentRegistry, LoggingDeprecationHandler.INSTANCE, stream)) {
                resultListener.onResponse(DataFrameTransformStateAndStats.fromXContent(parser));
            } catch (Exception e) {
                logger.error(
                    DataFrameMessages.getMessage(DataFrameMessages.FAILED_TO_PARSE_TRANSFORM_STATISTICS_CONFIGURATION, transformId), e);
                resultListener.onFailure(e);
            }
        }, e -> {
            if (e instanceof ResourceNotFoundException) {
                resultListener.onFailure(new ResourceNotFoundException(
                    DataFrameMessages.getMessage(DataFrameMessages.DATA_FRAME_UNKNOWN_TRANSFORM_STATS, transformId)));
            } else {
                resultListener.onFailure(e);
            }
        }));
    }

    public void getTransformStats(Collection<String> transformIds, ActionListener<List<DataFrameTransformStateAndStats>> listener) {

        QueryBuilder builder = QueryBuilders.constantScoreQuery(QueryBuilders.boolQuery()
                .filter(QueryBuilders.termsQuery(DataFrameField.ID.getPreferredName(), transformIds))
                .filter(QueryBuilders.termQuery(DataFrameField.INDEX_DOC_TYPE.getPreferredName(), DataFrameTransformStateAndStats.NAME)));

        SearchRequest searchRequest = client.prepareSearch(DataFrameInternalIndex.INDEX_NAME)
                .addSort(DataFrameField.ID.getPreferredName(), SortOrder.ASC)
                .setQuery(builder)
                .request();

        executeAsyncWithOrigin(client.threadPool().getThreadContext(), DATA_FRAME_ORIGIN, searchRequest,
                ActionListener.<SearchResponse>wrap(
                        searchResponse -> {
                            List<DataFrameTransformStateAndStats> stats = new ArrayList<>();
                            for (SearchHit hit : searchResponse.getHits().getHits()) {
                                BytesReference source = hit.getSourceRef();
                                try (InputStream stream = source.streamInput();
                                     XContentParser parser = XContentFactory.xContent(XContentType.JSON)
                                             .createParser(NamedXContentRegistry.EMPTY, LoggingDeprecationHandler.INSTANCE, stream)) {
                                    stats.add(DataFrameTransformStateAndStats.fromXContent(parser));
                                } catch (IOException e) {
                                    listener.onFailure(
                                            new ElasticsearchParseException("failed to parse data frame stats from search hit", e));
                                    return;
                                }
                            }

                            listener.onResponse(stats);
                        },
                        e -> {
                            if (e.getClass() == IndexNotFoundException.class) {
                                listener.onResponse(Collections.emptyList());
                            } else {
                                listener.onFailure(e);
                            }
                        }
                ), client::search);
    }

    private void parseTransformLenientlyFromSource(BytesReference source, String transformId,
            ActionListener<DataFrameTransformConfig> transformListener) {
        try (InputStream stream = source.streamInput();
                XContentParser parser = XContentFactory.xContent(XContentType.JSON)
                     .createParser(xContentRegistry, LoggingDeprecationHandler.INSTANCE, stream)) {
            transformListener.onResponse(DataFrameTransformConfig.fromXContent(parser, transformId, true));
        } catch (Exception e) {
            logger.error(DataFrameMessages.getMessage(DataFrameMessages.FAILED_TO_PARSE_TRANSFORM_CONFIGURATION, transformId), e);
            transformListener.onFailure(e);
        }
    }

    private void parseCheckpointsLenientlyFromSource(BytesReference source, String transformId,
            ActionListener<DataFrameTransformCheckpoint> transformListener) {
        try (InputStream stream = source.streamInput();
                XContentParser parser = XContentFactory.xContent(XContentType.JSON)
                     .createParser(xContentRegistry, LoggingDeprecationHandler.INSTANCE, stream)) {
            transformListener.onResponse(DataFrameTransformCheckpoint.fromXContent(parser, true));
        } catch (Exception e) {
            logger.error(DataFrameMessages.getMessage(DataFrameMessages.FAILED_TO_PARSE_TRANSFORM_CHECKPOINTS, transformId), e);
            transformListener.onFailure(e);
        }
    }

    private QueryBuilder buildQueryFromTokenizedIds(String[] idTokens, String resourceName) {
        BoolQueryBuilder queryBuilder = QueryBuilders.boolQuery()
            .filter(QueryBuilders.termQuery(DataFrameField.INDEX_DOC_TYPE.getPreferredName(), resourceName));
        if (Strings.isAllOrWildcard(idTokens) == false) {
            List<String> terms = new ArrayList<>();
            BoolQueryBuilder shouldQueries = new BoolQueryBuilder();
            for (String token : idTokens) {
                if (Regex.isSimpleMatchPattern(token)) {
                    shouldQueries.should(QueryBuilders.wildcardQuery(DataFrameField.ID.getPreferredName(), token));
                } else {
                    terms.add(token);
                }
            }
            if (terms.isEmpty() == false) {
                shouldQueries.should(QueryBuilders.termsQuery(DataFrameField.ID.getPreferredName(), terms));
            }

            if (shouldQueries.should().isEmpty() == false) {
                queryBuilder.filter(shouldQueries);
            }
        }
        return QueryBuilders.constantScoreQuery(queryBuilder);
    }
}
