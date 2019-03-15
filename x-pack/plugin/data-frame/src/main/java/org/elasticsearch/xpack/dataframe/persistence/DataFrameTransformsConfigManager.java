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
import org.elasticsearch.action.DocWriteResponse;
import org.elasticsearch.action.delete.DeleteAction;
import org.elasticsearch.action.delete.DeleteRequest;
import org.elasticsearch.action.get.GetAction;
import org.elasticsearch.action.get.GetRequest;
import org.elasticsearch.action.index.IndexAction;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.support.IndicesOptions;
import org.elasticsearch.action.support.WriteRequest;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.bytes.BytesReference;
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
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.xpack.core.ClientHelper;
import org.elasticsearch.xpack.core.dataframe.DataFrameField;
import org.elasticsearch.xpack.core.dataframe.DataFrameMessages;
import org.elasticsearch.xpack.core.dataframe.transforms.DataFrameTransformConfig;

import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.function.Consumer;

import static org.elasticsearch.xpack.core.ClientHelper.DATA_FRAME_ORIGIN;
import static org.elasticsearch.xpack.core.ClientHelper.executeAsyncWithOrigin;

public class DataFrameTransformsConfigManager {

    private static final Logger logger = LogManager.getLogger(DataFrameTransformsConfigManager.class);
    private static final int DEFAULT_SIZE = 100;

    public static final Map<String, String> TO_XCONTENT_PARAMS = Collections.singletonMap(DataFrameField.FOR_INTERNAL_STORAGE, "true");

    private final Client client;
    private final NamedXContentRegistry xContentRegistry;

    public DataFrameTransformsConfigManager(Client client, NamedXContentRegistry xContentRegistry) {
        this.client = client;
        this.xContentRegistry = xContentRegistry;
    }

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
     * Get more than one DataFrameTransformConfig
     *
     * @param transformId Can be a single transformId, `*`, or `_all`
     * @param resultListener Listener to alert when request is completed
     */
    // TODO add pagination support
    public void getTransformConfigurations(String transformId,
                                           ActionListener<List<DataFrameTransformConfig>> resultListener) {
        final boolean isAllOrWildCard = Strings.isAllOrWildcard(new String[]{transformId});
        BoolQueryBuilder queryBuilder = QueryBuilders.boolQuery()
            .filter(QueryBuilders.termQuery("doc_type", DataFrameTransformConfig.NAME));
        if (isAllOrWildCard == false) {
            queryBuilder.filter(QueryBuilders.termQuery(DataFrameField.ID.getPreferredName(), transformId));
        }

        SearchRequest request = client.prepareSearch(DataFrameInternalIndex.INDEX_NAME)
            .setIndicesOptions(IndicesOptions.LENIENT_EXPAND_OPEN)
            .setTrackTotalHits(true)
            .setSize(DEFAULT_SIZE)
            .setQuery(queryBuilder)
            .request();

        ClientHelper.executeAsyncWithOrigin(client.threadPool().getThreadContext(), DATA_FRAME_ORIGIN, request,
            ActionListener.<SearchResponse>wrap(
                searchResponse -> {
                    List<DataFrameTransformConfig> configs = new ArrayList<>(searchResponse.getHits().getHits().length);
                    for (SearchHit hit : searchResponse.getHits().getHits()) {
                        DataFrameTransformConfig config = parseTransformLenientlyFromSourceSync(hit.getSourceRef(),
                            resultListener::onFailure);
                        if (config == null) {
                            return;
                        }
                        configs.add(config);
                    }
                    if (configs.isEmpty() && (isAllOrWildCard == false)) {
                        resultListener.onFailure(new ResourceNotFoundException(
                            DataFrameMessages.getMessage(DataFrameMessages.REST_DATA_FRAME_UNKNOWN_TRANSFORM, transformId)));
                        return;
                    }
                    resultListener.onResponse(configs);
                },
                resultListener::onFailure)
        , client::search);

    }

    public void deleteTransformConfiguration(String transformId, ActionListener<Boolean> listener) {
        DeleteRequest request = new DeleteRequest(DataFrameInternalIndex.INDEX_NAME, DataFrameTransformConfig.documentId(transformId));
        request.setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE);

        executeAsyncWithOrigin(client, DATA_FRAME_ORIGIN, DeleteAction.INSTANCE, request, ActionListener.wrap(deleteResponse -> {

            if (deleteResponse.getResult() == DocWriteResponse.Result.NOT_FOUND) {
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

    private DataFrameTransformConfig parseTransformLenientlyFromSourceSync(BytesReference source, Consumer<Exception> onFailure) {
        try (InputStream stream = source.streamInput();
             XContentParser parser = XContentFactory.xContent(XContentType.JSON)
                 .createParser(xContentRegistry, LoggingDeprecationHandler.INSTANCE, stream)) {
            return DataFrameTransformConfig.fromXContent(parser, null, true);
        } catch (Exception e) {
            logger.error(DataFrameMessages.getMessage(DataFrameMessages.FAILED_TO_PARSE_TRANSFORM_CONFIGURATION), e);
            onFailure.accept(e);
            return null;
        }
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
}
