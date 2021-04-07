/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.ml.inference.persistence;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.message.ParameterizedMessage;
import org.elasticsearch.ResourceNotFoundException;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.search.SearchAction;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchRequestBuilder;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.CheckedConsumer;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.xcontent.LoggingDeprecationHandler;
import org.elasticsearch.common.xcontent.NamedXContentRegistry;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.sort.SortBuilders;
import org.elasticsearch.search.sort.SortOrder;
import org.elasticsearch.xpack.core.ml.inference.TrainedModelConfig;
import org.elasticsearch.xpack.core.ml.inference.persistence.InferenceIndexConstants;
import org.elasticsearch.xpack.core.ml.job.messages.Messages;
import org.elasticsearch.xpack.core.ml.utils.ExceptionsHelper;
import org.elasticsearch.xpack.ml.MachineLearning;

import java.io.IOException;
import java.io.InputStream;
import java.util.function.Consumer;

import static org.elasticsearch.xpack.core.ClientHelper.ML_ORIGIN;
import static org.elasticsearch.xpack.core.ClientHelper.executeAsyncWithOrigin;

/**
 * This is a use once class it has internal state to track progress
 */
public class ChunkedTrainedModelRestorer {

    private static final Logger logger = LogManager.getLogger(ChunkedTrainedModelRestorer.class);

    private static final int MAX_NUM_DEFINITION_DOCS = 20;

    private final Client client;
    private final NamedXContentRegistry xContentRegistry;
    private final String modelId;
    private int searchSize = 10;
    private int numDocsWritten = 0;

    public ChunkedTrainedModelRestorer(String modelId, Client client,
                                       NamedXContentRegistry xContentRegistry) {
        this.client = client;
        this.xContentRegistry = xContentRegistry;
        this.modelId = modelId;
    }

    public void setSearchSize(int searchSize) {
        if (searchSize > MAX_NUM_DEFINITION_DOCS) {
            throw new IllegalArgumentException("search size [" + searchSize + "] cannot be bigger than [" + MAX_NUM_DEFINITION_DOCS + "]");
        }
        this.searchSize = searchSize;
    }

    public int getNumDocsWritten() {
        return numDocsWritten;
    }

    public void restoreModelDefinition(CheckedConsumer<TrainedModelDefinitionDoc, IOException> modelConsumer,
                                       Consumer<Boolean> successConsumer,
                                       Consumer<Exception> errorConsumer) {

        SearchRequest searchRequest = buildSearch(client, modelId, searchSize);

        client.threadPool().executor(MachineLearning.UTILITY_THREAD_POOL_NAME).execute(() ->
            doSearch(searchRequest, modelConsumer, successConsumer, errorConsumer));
    }

    private void doSearch(SearchRequest searchRequest,
                          CheckedConsumer<TrainedModelDefinitionDoc, IOException> modelConsumer,
                          Consumer<Boolean> successConsumer,
                          Consumer<Exception> errorConsumer) {

        executeAsyncWithOrigin(client, ML_ORIGIN, SearchAction.INSTANCE, searchRequest, ActionListener.wrap(
            searchResponse -> {
                if (searchResponse.getHits().getHits().length == 0) {
                    errorConsumer.accept(new ResourceNotFoundException(
                        Messages.getMessage(Messages.MODEL_DEFINITION_NOT_FOUND, modelId)));
                    return;
                }

                for (SearchHit hit : searchResponse.getHits().getHits()) {
                    try {
                        modelConsumer.accept(parseModelDefinitionDocLenientlyFromSource(hit.getSourceRef(), modelId, xContentRegistry));
                    } catch (IOException e) {
                        logger.error(new ParameterizedMessage("[{}] error writing model definition", modelId), e);
                        errorConsumer.accept(e);
                        return;
                    }
                }

                numDocsWritten += searchResponse.getHits().getHits().length;

                if (searchResponse.getHits().getHits().length < searchSize) {
                    // end of search
                    successConsumer.accept(Boolean.TRUE);
                } else {
                    // search again with after
                    SearchHit lastHit = searchResponse.getHits().getAt(searchResponse.getHits().getHits().length -1);
                    SearchRequestBuilder searchRequestBuilder = buildSearchBuilder(client, modelId, searchSize);
                    searchRequestBuilder.searchAfter(new Object[]{lastHit.getIndex(), lastHit.getId()});
                    client.threadPool().executor(MachineLearning.UTILITY_THREAD_POOL_NAME).execute(() ->
                        doSearch(searchRequestBuilder.request(), modelConsumer, successConsumer, errorConsumer));
                }
            },
            e -> {
                if (ExceptionsHelper.unwrapCause(e) instanceof ResourceNotFoundException) {
                    errorConsumer.accept(new ResourceNotFoundException(
                        Messages.getMessage(Messages.MODEL_DEFINITION_NOT_FOUND, modelId)));
                } else {
                    errorConsumer.accept(e);
                }
            }
        ));
    }

    private static SearchRequestBuilder buildSearchBuilder(Client client, String modelId, int searchSize) {
        return client.prepareSearch(InferenceIndexConstants.INDEX_PATTERN)
            .setQuery(QueryBuilders.constantScoreQuery(QueryBuilders
                .boolQuery()
                .filter(QueryBuilders.termQuery(TrainedModelConfig.MODEL_ID.getPreferredName(), modelId))
                .filter(QueryBuilders.termQuery(InferenceIndexConstants.DOC_TYPE.getPreferredName(),
                    TrainedModelDefinitionDoc.NAME))))
            .setSize(searchSize)
            .setTrackTotalHits(false)
            // First find the latest index
            .addSort("_index", SortOrder.DESC)
            // Then, sort by doc_num
            .addSort(SortBuilders.fieldSort(TrainedModelDefinitionDoc.DOC_NUM.getPreferredName())
                .order(SortOrder.ASC)
                .unmappedType("long"));
    }

    public static SearchRequest buildSearch(Client client, String modelId, int searchSize) {
        return buildSearchBuilder(client, modelId, searchSize).request();
    }

    public static TrainedModelDefinitionDoc parseModelDefinitionDocLenientlyFromSource(BytesReference source,
                                                                                       String modelId,
                                                                                       NamedXContentRegistry xContentRegistry)
        throws IOException {

        try (InputStream stream = source.streamInput();
             XContentParser parser = XContentFactory.xContent(XContentType.JSON)
                 .createParser(xContentRegistry, LoggingDeprecationHandler.INSTANCE, stream)) {
            return TrainedModelDefinitionDoc.fromXContent(parser, true).build();
        } catch (IOException e) {
            logger.error(new ParameterizedMessage("[{}] failed to parse model definition", modelId), e);
            throw e;
        }
    }
}
