/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.transform.persistence;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.ElasticsearchParseException;
import org.elasticsearch.ElasticsearchStatusException;
import org.elasticsearch.ResourceAlreadyExistsException;
import org.elasticsearch.ResourceNotFoundException;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.DocWriteRequest;
import org.elasticsearch.action.bulk.BulkItemResponse;
import org.elasticsearch.action.index.IndexAction;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.search.SearchAction;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.support.IndicesOptions;
import org.elasticsearch.action.support.WriteRequest;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.collect.Tuple;
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
import org.elasticsearch.index.reindex.AbstractBulkByScrollRequest;
import org.elasticsearch.index.reindex.BulkByScrollResponse;
import org.elasticsearch.index.reindex.DeleteByQueryAction;
import org.elasticsearch.index.reindex.DeleteByQueryRequest;
import org.elasticsearch.index.reindex.ScrollableHitSource;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.sort.SortOrder;
import org.elasticsearch.xpack.core.action.util.ExpandedIdsMatcher;
import org.elasticsearch.xpack.core.action.util.PageParams;
import org.elasticsearch.xpack.core.transform.TransformField;
import org.elasticsearch.xpack.core.transform.TransformMessages;
import org.elasticsearch.xpack.core.transform.transforms.TransformCheckpoint;
import org.elasticsearch.xpack.core.transform.transforms.TransformConfig;
import org.elasticsearch.xpack.core.transform.transforms.TransformStoredDoc;
import org.elasticsearch.xpack.core.transform.transforms.persistence.TransformInternalIndexConstants;

import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Collection;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;

import static org.elasticsearch.index.mapper.MapperService.SINGLE_MAPPING_NAME;
import static org.elasticsearch.xpack.core.ClientHelper.TRANSFORM_ORIGIN;
import static org.elasticsearch.xpack.core.ClientHelper.executeAsyncWithOrigin;

/**
 * Place of all interactions with the internal transforms index. For configuration and mappings see @link{TransformInternalIndex}
 *
 * Versioned Index:
 *
 * We wrap several indexes under 1 pattern: ".transform-internal-001", ".transform-internal-002", ".transform-internal-n" while
 * n is the _current_ version of the index. For BWC we also search in ".data-frame-internal-1", ".data-frame-internal-2"
 *
 * - all gets/reads and dbq as well are searches on all indexes, while last-one-wins, so the result with the highest version is uses
 * - all puts and updates go into the _current_ version of the index, in case of updates this can leave dups behind
 *
 * Duplicate handling / old version cleanup
 *
 * As we always write to the new index, updates of older documents leave a dup in the previous versioned index behind. However,
 * documents are tiny, so the impact is rather small.
 *
 * Nevertheless cleanup would be good, eventually we need to move old documents into new indexes after major upgrades.
 *
 * TODO: Provide a method that moves old docs into the current index and delete old indexes and templates
 */
public class IndexBasedTransformConfigManager implements TransformConfigManager {

    private static final Logger logger = LogManager.getLogger(IndexBasedTransformConfigManager.class);

    private final Client client;
    private final NamedXContentRegistry xContentRegistry;

    public IndexBasedTransformConfigManager(Client client, NamedXContentRegistry xContentRegistry) {
        this.client = client;
        this.xContentRegistry = xContentRegistry;
    }

    @Override
    public void putTransformCheckpoint(TransformCheckpoint checkpoint, ActionListener<Boolean> listener) {
        try (XContentBuilder builder = XContentFactory.jsonBuilder()) {
            XContentBuilder source = checkpoint.toXContent(builder, new ToXContent.MapParams(TO_XCONTENT_PARAMS));

            IndexRequest indexRequest = new IndexRequest(TransformInternalIndexConstants.LATEST_INDEX_NAME).opType(
                DocWriteRequest.OpType.INDEX
            )
                .setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE)
                .id(TransformCheckpoint.documentId(checkpoint.getTransformId(), checkpoint.getCheckpoint()))
                .source(source);

            executeAsyncWithOrigin(
                client,
                TRANSFORM_ORIGIN,
                IndexAction.INSTANCE,
                indexRequest,
                ActionListener.wrap(r -> { listener.onResponse(true); }, listener::onFailure)
            );
        } catch (IOException e) {
            // not expected to happen but for the sake of completeness
            listener.onFailure(e);
        }
    }

    @Override
    public void putTransformConfiguration(TransformConfig transformConfig, ActionListener<Boolean> listener) {
        putTransformConfiguration(transformConfig, DocWriteRequest.OpType.CREATE, null, listener);
    }

    @Override
    public void updateTransformConfiguration(
        TransformConfig transformConfig,
        SeqNoPrimaryTermAndIndex seqNoPrimaryTermAndIndex,
        ActionListener<Boolean> listener
    ) {
        if (seqNoPrimaryTermAndIndex.getIndex().equals(TransformInternalIndexConstants.LATEST_INDEX_NAME)) {
            // update the config in the same, current index using optimistic concurrency control
            putTransformConfiguration(transformConfig, DocWriteRequest.OpType.INDEX, seqNoPrimaryTermAndIndex, listener);
        } else {
            // create the config in the current version of the index assuming there is no existing one
            // this leaves a dup behind in the old index, see dup handling on the top
            putTransformConfiguration(transformConfig, DocWriteRequest.OpType.CREATE, null, listener);
        }
    }

    @Override
    public void deleteOldTransformConfigurations(String transformId, ActionListener<Boolean> listener) {
        DeleteByQueryRequest deleteByQueryRequest = createDeleteByQueryRequest();
        deleteByQueryRequest.indices(
            TransformInternalIndexConstants.INDEX_NAME_PATTERN,
            TransformInternalIndexConstants.INDEX_NAME_PATTERN_DEPRECATED
        );
        deleteByQueryRequest.setQuery(
            QueryBuilders.constantScoreQuery(
                QueryBuilders.boolQuery()
                    .mustNot(QueryBuilders.termQuery("_index", TransformInternalIndexConstants.LATEST_INDEX_NAME))
                    .filter(QueryBuilders.termQuery("_id", TransformConfig.documentId(transformId)))
            )
        );

        executeAsyncWithOrigin(
            client,
            TRANSFORM_ORIGIN,
            DeleteByQueryAction.INSTANCE,
            deleteByQueryRequest,
            ActionListener.wrap(response -> {
                if ((response.getBulkFailures().isEmpty() && response.getSearchFailures().isEmpty()) == false) {
                    Tuple<RestStatus, Throwable> statusAndReason = getStatusAndReason(response);
                    listener.onFailure(
                        new ElasticsearchStatusException(statusAndReason.v2().getMessage(), statusAndReason.v1(), statusAndReason.v2())
                    );
                    return;
                }
                listener.onResponse(true);
            }, listener::onFailure)
        );
    }

    @Override
    public void deleteOldTransformStoredDocuments(String transformId, ActionListener<Boolean> listener) {
        DeleteByQueryRequest deleteByQueryRequest = createDeleteByQueryRequest();
        deleteByQueryRequest.indices(
            TransformInternalIndexConstants.INDEX_NAME_PATTERN,
            TransformInternalIndexConstants.INDEX_NAME_PATTERN_DEPRECATED
        );
        deleteByQueryRequest.setQuery(
            QueryBuilders.constantScoreQuery(
                QueryBuilders.boolQuery()
                    .mustNot(QueryBuilders.termQuery("_index", TransformInternalIndexConstants.LATEST_INDEX_NAME))
                    .filter(QueryBuilders.termQuery("_id", TransformStoredDoc.documentId(transformId)))
            )
        );
        executeAsyncWithOrigin(
            client,
            TRANSFORM_ORIGIN,
            DeleteByQueryAction.INSTANCE,
            deleteByQueryRequest,
            ActionListener.wrap(response -> {
                if ((response.getBulkFailures().isEmpty() && response.getSearchFailures().isEmpty()) == false) {
                    Tuple<RestStatus, Throwable> statusAndReason = getStatusAndReason(response);
                    listener.onFailure(
                        new ElasticsearchStatusException(statusAndReason.v2().getMessage(), statusAndReason.v1(), statusAndReason.v2())
                    );
                    return;
                }
                listener.onResponse(true);
            }, listener::onFailure)
        );
    }

    @Override
    public void deleteOldCheckpoints(String transformId, long deleteCheckpointsBelow, long deleteOlderThan, ActionListener<Long> listener) {
        DeleteByQueryRequest deleteByQueryRequest = createDeleteByQueryRequest();
        deleteByQueryRequest.indices(
            TransformInternalIndexConstants.INDEX_NAME_PATTERN,
            TransformInternalIndexConstants.INDEX_NAME_PATTERN_DEPRECATED
        );
        deleteByQueryRequest.setQuery(
            QueryBuilders.boolQuery()
                .filter(QueryBuilders.termQuery(TransformField.ID.getPreferredName(), transformId))
                .filter(QueryBuilders.termQuery(TransformField.INDEX_DOC_TYPE.getPreferredName(), TransformCheckpoint.NAME))
                .filter(QueryBuilders.rangeQuery(TransformCheckpoint.CHECKPOINT.getPreferredName()).lt(deleteCheckpointsBelow))
                .filter(
                    QueryBuilders.rangeQuery(TransformField.TIMESTAMP_MILLIS.getPreferredName()).lt(deleteOlderThan).format("epoch_millis")
                )
        );
        logger.debug("Deleting old checkpoints using {}", deleteByQueryRequest.getSearchRequest());
        executeAsyncWithOrigin(
            client,
            TRANSFORM_ORIGIN,
            DeleteByQueryAction.INSTANCE,
            deleteByQueryRequest,
            ActionListener.wrap(response -> {
                if ((response.getBulkFailures().isEmpty() && response.getSearchFailures().isEmpty()) == false) {
                    Tuple<RestStatus, Throwable> statusAndReason = getStatusAndReason(response);
                    listener.onFailure(
                        new ElasticsearchStatusException(statusAndReason.v2().getMessage(), statusAndReason.v1(), statusAndReason.v2())
                    );
                    return;
                }
                listener.onResponse(response.getDeleted());
            }, listener::onFailure)
        );
    }

    private void putTransformConfiguration(
        TransformConfig transformConfig,
        DocWriteRequest.OpType optType,
        SeqNoPrimaryTermAndIndex seqNoPrimaryTermAndIndex,
        ActionListener<Boolean> listener
    ) {
        try (XContentBuilder builder = XContentFactory.jsonBuilder()) {
            XContentBuilder source = transformConfig.toXContent(builder, new ToXContent.MapParams(TO_XCONTENT_PARAMS));

            IndexRequest indexRequest = new IndexRequest(TransformInternalIndexConstants.LATEST_INDEX_NAME).opType(optType)
                .setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE)
                .id(TransformConfig.documentId(transformConfig.getId()))
                .source(source);
            if (seqNoPrimaryTermAndIndex != null) {
                indexRequest.setIfSeqNo(seqNoPrimaryTermAndIndex.getSeqNo()).setIfPrimaryTerm(seqNoPrimaryTermAndIndex.getPrimaryTerm());
            }
            executeAsyncWithOrigin(
                client,
                TRANSFORM_ORIGIN,
                IndexAction.INSTANCE,
                indexRequest,
                ActionListener.wrap(r -> { listener.onResponse(true); }, e -> {
                    if (e instanceof VersionConflictEngineException) {
                        // the transform already exists
                        listener.onFailure(
                            new ResourceAlreadyExistsException(
                                TransformMessages.getMessage(TransformMessages.REST_PUT_TRANSFORM_EXISTS, transformConfig.getId())
                            )
                        );
                    } else {
                        listener.onFailure(new RuntimeException(TransformMessages.REST_PUT_FAILED_PERSIST_TRANSFORM_CONFIGURATION, e));
                    }
                })
            );
        } catch (IOException e) {
            // not expected to happen but for the sake of completeness
            listener.onFailure(
                new ElasticsearchParseException(
                    TransformMessages.getMessage(TransformMessages.REST_FAILED_TO_SERIALIZE_TRANSFORM, transformConfig.getId()),
                    e
                )
            );
        }
    }

    @Override
    public void getTransformCheckpoint(String transformId, long checkpoint, ActionListener<TransformCheckpoint> resultListener) {
        QueryBuilder queryBuilder = QueryBuilders.termQuery("_id", TransformCheckpoint.documentId(transformId, checkpoint));
        SearchRequest searchRequest = client.prepareSearch(
            TransformInternalIndexConstants.INDEX_NAME_PATTERN,
            TransformInternalIndexConstants.INDEX_NAME_PATTERN_DEPRECATED
        )
            .setQuery(queryBuilder)
            // use sort to get the last
            .addSort("_index", SortOrder.DESC)
            .setSize(1)
            .setAllowPartialSearchResults(false)
            .request();

        executeAsyncWithOrigin(
            client,
            TRANSFORM_ORIGIN,
            SearchAction.INSTANCE,
            searchRequest,
            ActionListener.<SearchResponse>wrap(searchResponse -> {
                if (searchResponse.getHits().getHits().length == 0) {
                    // do not fail if checkpoint does not exist but return an empty checkpoint
                    logger.trace("found no checkpoint for transform [" + transformId + "], returning empty checkpoint");
                    resultListener.onResponse(TransformCheckpoint.EMPTY);
                    return;
                }
                BytesReference source = searchResponse.getHits().getHits()[0].getSourceRef();
                parseCheckpointsLenientlyFromSource(source, transformId, resultListener);
            }, resultListener::onFailure)
        );
    }

    @Override
    public void getTransformConfiguration(String transformId, ActionListener<TransformConfig> resultListener) {
        QueryBuilder queryBuilder = QueryBuilders.termQuery("_id", TransformConfig.documentId(transformId));
        SearchRequest searchRequest = client.prepareSearch(
            TransformInternalIndexConstants.INDEX_NAME_PATTERN,
            TransformInternalIndexConstants.INDEX_NAME_PATTERN_DEPRECATED
        )
            .setQuery(queryBuilder)
            // use sort to get the last
            .addSort("_index", SortOrder.DESC)
            .setSize(1)
            .setAllowPartialSearchResults(false)
            .request();

        executeAsyncWithOrigin(
            client,
            TRANSFORM_ORIGIN,
            SearchAction.INSTANCE,
            searchRequest,
            ActionListener.<SearchResponse>wrap(searchResponse -> {
                if (searchResponse.getHits().getHits().length == 0) {
                    resultListener.onFailure(
                        new ResourceNotFoundException(TransformMessages.getMessage(TransformMessages.REST_UNKNOWN_TRANSFORM, transformId))
                    );
                    return;
                }
                BytesReference source = searchResponse.getHits().getHits()[0].getSourceRef();
                parseTransformLenientlyFromSource(source, transformId, resultListener);
            }, resultListener::onFailure)
        );
    }

    @Override
    public void getTransformConfigurationForUpdate(
        String transformId,
        ActionListener<Tuple<TransformConfig, SeqNoPrimaryTermAndIndex>> configAndVersionListener
    ) {
        QueryBuilder queryBuilder = QueryBuilders.termQuery("_id", TransformConfig.documentId(transformId));
        SearchRequest searchRequest = client.prepareSearch(
            TransformInternalIndexConstants.INDEX_NAME_PATTERN,
            TransformInternalIndexConstants.INDEX_NAME_PATTERN_DEPRECATED
        )
            .setQuery(queryBuilder)
            // use sort to get the last
            .addSort("_index", SortOrder.DESC)
            .setSize(1)
            .setAllowPartialSearchResults(false)
            .seqNoAndPrimaryTerm(true)
            .request();

        executeAsyncWithOrigin(client, TRANSFORM_ORIGIN, SearchAction.INSTANCE, searchRequest, ActionListener.wrap(searchResponse -> {
            if (searchResponse.getHits().getHits().length == 0) {
                configAndVersionListener.onFailure(
                    new ResourceNotFoundException(TransformMessages.getMessage(TransformMessages.REST_UNKNOWN_TRANSFORM, transformId))
                );
                return;
            }
            SearchHit hit = searchResponse.getHits().getHits()[0];
            BytesReference source = hit.getSourceRef();
            parseTransformLenientlyFromSource(
                source,
                transformId,
                ActionListener.wrap(
                    config -> configAndVersionListener.onResponse(
                        Tuple.tuple(config, new SeqNoPrimaryTermAndIndex(hit.getSeqNo(), hit.getPrimaryTerm(), hit.getIndex()))
                    ),
                    configAndVersionListener::onFailure
                )
            );
        }, configAndVersionListener::onFailure));
    }

    @Override
    public void expandTransformIds(
        String transformIdsExpression,
        PageParams pageParams,
        boolean allowNoMatch,
        ActionListener<Tuple<Long, List<String>>> foundIdsListener
    ) {
        String[] idTokens = ExpandedIdsMatcher.tokenizeExpression(transformIdsExpression);
        QueryBuilder queryBuilder = buildQueryFromTokenizedIds(idTokens, TransformConfig.NAME);

        SearchRequest request = client.prepareSearch(
            TransformInternalIndexConstants.INDEX_NAME_PATTERN,
            TransformInternalIndexConstants.INDEX_NAME_PATTERN_DEPRECATED
        )
            .addSort(TransformField.ID.getPreferredName(), SortOrder.ASC)
            .setFrom(pageParams.getFrom())
            .setTrackTotalHits(true)
            .setSize(pageParams.getSize())
            .setQuery(queryBuilder)
            // We only care about the `id` field, small optimization
            .setFetchSource(TransformField.ID.getPreferredName(), "")
            .request();

        final ExpandedIdsMatcher requiredMatches = new ExpandedIdsMatcher(idTokens, allowNoMatch);

        executeAsyncWithOrigin(
            client.threadPool().getThreadContext(),
            TRANSFORM_ORIGIN,
            request,
            ActionListener.<SearchResponse>wrap(searchResponse -> {
                long totalHits = searchResponse.getHits().getTotalHits().value;
                // important: preserve order
                Set<String> ids = new LinkedHashSet<>(searchResponse.getHits().getHits().length);
                for (SearchHit hit : searchResponse.getHits().getHits()) {
                    BytesReference source = hit.getSourceRef();
                    try (
                        InputStream stream = source.streamInput();
                        XContentParser parser = XContentFactory.xContent(XContentType.JSON)
                            .createParser(NamedXContentRegistry.EMPTY, LoggingDeprecationHandler.INSTANCE, stream)
                    ) {
                        ids.add((String) parser.map().get(TransformField.ID.getPreferredName()));
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
                            TransformMessages.getMessage(TransformMessages.REST_UNKNOWN_TRANSFORM, requiredMatches.unmatchedIdsString())
                        )
                    );
                    return;
                }
                foundIdsListener.onResponse(new Tuple<>(totalHits, new ArrayList<>(ids)));
            }, foundIdsListener::onFailure),
            client::search
        );
    }

    @Override
    public void deleteTransform(String transformId, ActionListener<Boolean> listener) {
        DeleteByQueryRequest request = createDeleteByQueryRequest();

        request.indices(TransformInternalIndexConstants.INDEX_NAME_PATTERN, TransformInternalIndexConstants.INDEX_NAME_PATTERN_DEPRECATED);
        QueryBuilder query = QueryBuilders.termQuery(TransformField.ID.getPreferredName(), transformId);
        request.setQuery(query);
        request.setRefresh(true);

        executeAsyncWithOrigin(client, TRANSFORM_ORIGIN, DeleteByQueryAction.INSTANCE, request, ActionListener.wrap(deleteResponse -> {
            if (deleteResponse.getDeleted() == 0) {
                listener.onFailure(
                    new ResourceNotFoundException(TransformMessages.getMessage(TransformMessages.REST_UNKNOWN_TRANSFORM, transformId))
                );
                return;
            }
            listener.onResponse(true);
        }, e -> {
            if (e.getClass() == IndexNotFoundException.class) {
                listener.onFailure(
                    new ResourceNotFoundException(TransformMessages.getMessage(TransformMessages.REST_UNKNOWN_TRANSFORM, transformId))
                );
            } else {
                listener.onFailure(e);
            }
        }));
    }

    @Override
    public void putOrUpdateTransformStoredDoc(
        TransformStoredDoc storedDoc,
        SeqNoPrimaryTermAndIndex seqNoPrimaryTermAndIndex,
        ActionListener<SeqNoPrimaryTermAndIndex> listener
    ) {
        try (XContentBuilder builder = XContentFactory.jsonBuilder()) {
            XContentBuilder source = storedDoc.toXContent(builder, new ToXContent.MapParams(TO_XCONTENT_PARAMS));

            IndexRequest indexRequest = new IndexRequest(TransformInternalIndexConstants.LATEST_INDEX_NAME).setRefreshPolicy(
                WriteRequest.RefreshPolicy.IMMEDIATE
            ).id(TransformStoredDoc.documentId(storedDoc.getId())).source(source);
            if (seqNoPrimaryTermAndIndex != null
                && seqNoPrimaryTermAndIndex.getIndex().equals(TransformInternalIndexConstants.LATEST_INDEX_NAME)) {
                indexRequest.opType(DocWriteRequest.OpType.INDEX)
                    .setIfSeqNo(seqNoPrimaryTermAndIndex.getSeqNo())
                    .setIfPrimaryTerm(seqNoPrimaryTermAndIndex.getPrimaryTerm());
            } else {
                // If the index is NOT the latest or we are null, that means we have not created this doc before
                // so, it should be a create option without the seqNo and primaryTerm set
                indexRequest.opType(DocWriteRequest.OpType.CREATE);
            }
            executeAsyncWithOrigin(
                client,
                TRANSFORM_ORIGIN,
                IndexAction.INSTANCE,
                indexRequest,
                ActionListener.wrap(
                    r -> listener.onResponse(SeqNoPrimaryTermAndIndex.fromIndexResponse(r)),
                    e -> listener.onFailure(
                        new RuntimeException(
                            TransformMessages.getMessage(TransformMessages.TRANSFORM_FAILED_TO_PERSIST_STATS, storedDoc.getId()),
                            e
                        )
                    )
                )
            );
        } catch (IOException e) {
            // not expected to happen but for the sake of completeness
            listener.onFailure(
                new ElasticsearchParseException(
                    TransformMessages.getMessage(TransformMessages.TRANSFORM_FAILED_TO_PERSIST_STATS, storedDoc.getId()),
                    e
                )
            );
        }
    }

    @Override
    public void getTransformStoredDoc(
        String transformId,
        ActionListener<Tuple<TransformStoredDoc, SeqNoPrimaryTermAndIndex>> resultListener
    ) {
        QueryBuilder queryBuilder = QueryBuilders.termQuery("_id", TransformStoredDoc.documentId(transformId));
        SearchRequest searchRequest = client.prepareSearch(
            TransformInternalIndexConstants.INDEX_NAME_PATTERN,
            TransformInternalIndexConstants.INDEX_NAME_PATTERN_DEPRECATED
        )
            .setQuery(queryBuilder)
            // use sort to get the last
            .addSort("_index", SortOrder.DESC)
            .setSize(1)
            .setAllowPartialSearchResults(false)
            .seqNoAndPrimaryTerm(true)
            .request();

        executeAsyncWithOrigin(
            client,
            TRANSFORM_ORIGIN,
            SearchAction.INSTANCE,
            searchRequest,
            ActionListener.<SearchResponse>wrap(searchResponse -> {
                if (searchResponse.getHits().getHits().length == 0) {
                    resultListener.onFailure(
                        new ResourceNotFoundException(TransformMessages.getMessage(TransformMessages.UNKNOWN_TRANSFORM_STATS, transformId))
                    );
                    return;
                }
                SearchHit searchHit = searchResponse.getHits().getHits()[0];
                BytesReference source = searchHit.getSourceRef();
                try (
                    InputStream stream = source.streamInput();
                    XContentParser parser = XContentFactory.xContent(XContentType.JSON)
                        .createParser(xContentRegistry, LoggingDeprecationHandler.INSTANCE, stream)
                ) {
                    resultListener.onResponse(
                        Tuple.tuple(TransformStoredDoc.fromXContent(parser), SeqNoPrimaryTermAndIndex.fromSearchHit(searchHit))
                    );
                } catch (Exception e) {
                    logger.error(
                        TransformMessages.getMessage(TransformMessages.FAILED_TO_PARSE_TRANSFORM_STATISTICS_CONFIGURATION, transformId),
                        e
                    );
                    resultListener.onFailure(e);
                }
            }, resultListener::onFailure)
        );
    }

    @Override
    public void getTransformStoredDocs(Collection<String> transformIds, ActionListener<List<TransformStoredDoc>> listener) {
        QueryBuilder builder = QueryBuilders.constantScoreQuery(
            QueryBuilders.boolQuery()
                .filter(QueryBuilders.termsQuery(TransformField.ID.getPreferredName(), transformIds))
                .filter(QueryBuilders.termQuery(TransformField.INDEX_DOC_TYPE.getPreferredName(), TransformStoredDoc.NAME))
        );

        SearchRequest searchRequest = client.prepareSearch(
            TransformInternalIndexConstants.INDEX_NAME_PATTERN,
            TransformInternalIndexConstants.INDEX_NAME_PATTERN_DEPRECATED
        )
            .addSort(TransformField.ID.getPreferredName(), SortOrder.ASC)
            .addSort("_index", SortOrder.DESC)
            .setQuery(builder)
            // the limit for getting stats and transforms is 1000, as long as we do not have 10 indices this works
            .setSize(Math.min(transformIds.size(), 10_000))
            .request();

        executeAsyncWithOrigin(
            client.threadPool().getThreadContext(),
            TRANSFORM_ORIGIN,
            searchRequest,
            ActionListener.<SearchResponse>wrap(searchResponse -> {
                List<TransformStoredDoc> stats = new ArrayList<>();
                String previousId = null;
                for (SearchHit hit : searchResponse.getHits().getHits()) {
                    // skip old versions
                    if (hit.getId().equals(previousId) == false) {
                        previousId = hit.getId();
                        BytesReference source = hit.getSourceRef();
                        try (
                            InputStream stream = source.streamInput();
                            XContentParser parser = XContentFactory.xContent(XContentType.JSON)
                                .createParser(NamedXContentRegistry.EMPTY, LoggingDeprecationHandler.INSTANCE, stream)
                        ) {
                            stats.add(TransformStoredDoc.fromXContent(parser));
                        } catch (IOException e) {
                            listener.onFailure(new ElasticsearchParseException("failed to parse transform stats from search hit", e));
                            return;
                        }
                    }
                }

                listener.onResponse(stats);
            }, listener::onFailure),
            client::search
        );
    }

    private void parseTransformLenientlyFromSource(
        BytesReference source,
        String transformId,
        ActionListener<TransformConfig> transformListener
    ) {
        try (
            InputStream stream = source.streamInput();
            XContentParser parser = XContentFactory.xContent(XContentType.JSON)
                .createParser(xContentRegistry, LoggingDeprecationHandler.INSTANCE, stream)
        ) {
            transformListener.onResponse(TransformConfig.fromXContent(parser, transformId, true));
        } catch (Exception e) {
            logger.error(TransformMessages.getMessage(TransformMessages.FAILED_TO_PARSE_TRANSFORM_CONFIGURATION, transformId), e);
            transformListener.onFailure(e);
        }
    }

    private void parseCheckpointsLenientlyFromSource(
        BytesReference source,
        String transformId,
        ActionListener<TransformCheckpoint> transformListener
    ) {
        try (
            InputStream stream = source.streamInput();
            XContentParser parser = XContentFactory.xContent(XContentType.JSON)
                .createParser(xContentRegistry, LoggingDeprecationHandler.INSTANCE, stream)
        ) {
            transformListener.onResponse(TransformCheckpoint.fromXContent(parser, true));
        } catch (Exception e) {
            logger.error(TransformMessages.getMessage(TransformMessages.FAILED_TO_PARSE_TRANSFORM_CHECKPOINTS, transformId), e);
            transformListener.onFailure(e);
        }
    }

    private QueryBuilder buildQueryFromTokenizedIds(String[] idTokens, String resourceName) {
        BoolQueryBuilder queryBuilder = QueryBuilders.boolQuery()
            .filter(QueryBuilders.termQuery(TransformField.INDEX_DOC_TYPE.getPreferredName(), resourceName));
        if (Strings.isAllOrWildcard(idTokens) == false) {
            List<String> terms = new ArrayList<>();
            BoolQueryBuilder shouldQueries = new BoolQueryBuilder();
            for (String token : idTokens) {
                if (Regex.isSimpleMatchPattern(token)) {
                    shouldQueries.should(QueryBuilders.wildcardQuery(TransformField.ID.getPreferredName(), token));
                } else {
                    terms.add(token);
                }
            }
            if (terms.isEmpty() == false) {
                shouldQueries.should(QueryBuilders.termsQuery(TransformField.ID.getPreferredName(), terms));
            }

            if (shouldQueries.should().isEmpty() == false) {
                queryBuilder.filter(shouldQueries);
            }
        }
        return QueryBuilders.constantScoreQuery(queryBuilder);
    }

    private static Tuple<RestStatus, Throwable> getStatusAndReason(final BulkByScrollResponse response) {
        RestStatus status = RestStatus.OK;
        Throwable reason = new Exception("Unknown error");
        // Getting the max RestStatus is sort of arbitrary, would the user care about 5xx over 4xx?
        // Unsure of a better way to return an appropriate and possibly actionable cause to the user.
        for (BulkItemResponse.Failure failure : response.getBulkFailures()) {
            if (failure.getStatus().getStatus() > status.getStatus()) {
                status = failure.getStatus();
                reason = failure.getCause();
            }
        }

        for (ScrollableHitSource.SearchFailure failure : response.getSearchFailures()) {
            RestStatus failureStatus = org.elasticsearch.ExceptionsHelper.status(failure.getReason());
            if (failureStatus.getStatus() > status.getStatus()) {
                status = failureStatus;
                reason = failure.getReason();
            }
        }
        return new Tuple<>(status, reason);
    }

    /**
     * Create DBQ request with good defaults
     *
     * @return new DeleteByQueryRequest with some defaults set
     */
    private static DeleteByQueryRequest createDeleteByQueryRequest() {

        DeleteByQueryRequest deleteByQuery = new DeleteByQueryRequest();

        deleteByQuery.setAbortOnVersionConflict(false)
            .setSlices(AbstractBulkByScrollRequest.AUTO_SLICES)
            .setIndicesOptions(IndicesOptions.lenientExpandOpen());

        // disable scoring by using index order
        deleteByQuery.getSearchRequest().source().sort(SINGLE_MAPPING_NAME);
        return deleteByQuery;
    }
}
