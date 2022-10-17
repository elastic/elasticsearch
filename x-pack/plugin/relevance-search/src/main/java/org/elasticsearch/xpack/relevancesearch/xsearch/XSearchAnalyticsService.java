/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.relevancesearch.xsearch;

import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.DocWriteRequest;
import org.elasticsearch.action.index.IndexAction;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.client.internal.Requests;
import org.elasticsearch.client.internal.node.NodeClient;
import org.elasticsearch.cluster.metadata.DataStream;
import org.elasticsearch.cluster.metadata.SearchEngine;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.logging.LogManager;
import org.elasticsearch.logging.Logger;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentFactory;
import org.elasticsearch.xpack.relevancesearch.xsearch.action.XSearchAction;

import java.io.IOException;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.util.Arrays;
import java.util.Collection;
import java.util.Map;

public class XSearchAnalyticsService {

    private static final Logger logger = LogManager.getLogger(XSearchAnalyticsService.class);

    private final ClusterService clusterService;

    public XSearchAnalyticsService(ClusterService clusterService) {
        this.clusterService = clusterService;
    }

    public void recordEvent(XSearchAction.Request request, NodeClient client) {
        Collection<SearchEngine> engines = getEnginesForRequest(request);
        Collection<String> dataStreams = getDataStreamsForEngines(engines);
        dataStreams.forEach(ds -> recordEvent(ds, request, client));
    }

    private void recordEvent(String dataStream, XSearchAction.Request request, NodeClient client) {
        try (XContentBuilder builder = buildEvent(request)) {
            if (builder != null) {
                IndexRequest indexRequest = client.prepareIndex(dataStream)
                    .setSource(buildEvent(request))
                    .request()
                    .opType(DocWriteRequest.OpType.CREATE);
                logger.info("INDEXING REQUEST " + indexRequest.toString());
                client.execute(IndexAction.INSTANCE, indexRequest, new ActionListener<IndexResponse>() {
                    @Override
                    public void onResponse(IndexResponse indexResponse) {
                        // No action required
                    }

                    @Override
                    public void onFailure(Exception e) {
                        logger.error("analytics indexing failure", e);
                    }
                });
            }
        } catch (Exception e) {
            logger.error("analytics indexing failure", e);
        }
    }

    private XContentBuilder buildEvent(XSearchAction.Request request) {
        try (XContentBuilder builder = XContentFactory.contentBuilder(Requests.INDEX_CONTENT_TYPE)) {
            builder.startObject();
            builder.field("@timestamp", ZonedDateTime.now(ZoneId.ofOffset("UTC", ZoneOffset.UTC)));
            builder.field("query", request.getQuery());
            builder.endObject();
            return builder;
        } catch (IOException e) {
            throw new ElasticsearchException("could not build analytics event", e);
        }
    }

    private Collection<String> getDataStreamsForEngines(Collection<SearchEngine> engines) {
        Map<String, DataStream> dataStreams = clusterService.state().metadata().dataStreams();
        return engines.stream()
            .filter(SearchEngine::shouldRecordAnalytics)
            .map(SearchEngine::getAnalyticsCollection)
            .filter(dataStreams::containsKey)
            .toList();
    }

    private Collection<SearchEngine> getEnginesForRequest(XSearchAction.Request request) {
        Map<String, SearchEngine> engines = clusterService.state().metadata().searchEngines();
        return Arrays.stream(request.indices()).filter(engines::containsKey).map(engines::get).toList();
    }

}
