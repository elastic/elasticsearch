/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.transform.transforms.map;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.search.aggregations.AggregationBuilder;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.xpack.core.transform.transforms.SourceConfig;
import org.elasticsearch.xpack.core.transform.transforms.TransformIndexerStats;
import org.elasticsearch.xpack.core.transform.transforms.map.MapConfig;
import org.elasticsearch.xpack.transform.transforms.ChangeCollector;
import org.elasticsearch.xpack.transform.transforms.Function;

import java.util.Arrays;
import java.util.Collections;
import java.util.stream.Stream;

public class Map implements Function {
    private static final Logger logger = LogManager.getLogger(Map.class);

    private final MapConfig mapConfig;
    private final String transformId;

    public Map(MapConfig mapConfig, String transformId) {
        this.mapConfig = mapConfig;
        this.transformId = transformId;
    }

    @Override
    public int getInitialPageSize() {
        return 5000;
    }

    @Override
    public SearchSourceBuilder source(SearchSourceBuilder builder, java.util.Map<String, Object> position, int pageSize) {
        builder.size(5000);
        if (position == null) {
            return builder;
        }
        return builder.searchAfter((Object[]) position.get("search_after"));
    }

    @Override
    public AggregationBuilder aggregation(java.util.Map<String, Object> position, int pageSize) {
        return null;
    }

    @Override
    public ChangeCollector buildChangeCollector(String synchronizationField) {
        return null;
    }

    @Override
    public boolean supportsIncrementalBucketUpdate() {
        return false;
    }

    @Override
    public Stream<IndexRequest> processBuckets(
        SearchResponse searchResponse,
        String destinationIndex,
        String destinationPipeline,
        java.util.Map<String, String> fieldMappings,
        TransformIndexerStats stats
    ) {
        logger.info("mapping buckets, total hits: {}", searchResponse.getHits().getTotalHits());

        return Arrays.stream(searchResponse.getHits().getHits()).map(hit -> {

            // logger.info("hit: {}", hit);
            BytesReference source = hit.getSourceRef();
            XContentType sourceType = XContentHelper.xContentType(source);

            IndexRequest request = new IndexRequest(destinationIndex).source(source, sourceType).id(hit.getId());
            if (destinationPipeline != null) {
                request.setPipeline(destinationPipeline);
            }
            return request;
        });
    }

    @Override
    public java.util.Map<String, Object> getAfterKey(SearchResponse searchResponse) {
        return Collections.singletonMap("search_after", searchResponse.getHits().getSortFields());
    }

    @Override
    public void validateQuery(Client client, SourceConfig sourceConfig, final ActionListener<Boolean> listener) {
        listener.onResponse(true);
    }

    @Override
    public void validateConfig() {
        // TODO Auto-generated method stub

    }

}
