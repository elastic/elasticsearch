/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.action.index;

import org.elasticsearch.action.RequestBuilder;
import org.elasticsearch.action.support.WriteRequest;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentType;

import java.util.Map;

public class IndexRequestBuilder2 implements RequestBuilder<IndexRequest> {
    private final String index;
    private String id = null;
    private Map<String, ?> sourceMap;
    private Object[] sourceArray;
    private XContentBuilder sourceXContentBuilder;
    private String sourceString;
    private XContentType sourceContentType;
    private String pipeline;
    private String routing;
    private WriteRequest.RefreshPolicy refreshPolicy;


    public IndexRequestBuilder2(String index) {
        this.index = index;
    }

    public IndexRequestBuilder2 id(String id) {
        this.id = id;
        return this;
    }

    public IndexRequestBuilder2 source(Map<String, ?> source) {
        this.sourceMap = source;
        return this;
    }

    public IndexRequestBuilder2 source(Object... source) {
        this.sourceArray = source;
        return this;
    }

    public IndexRequestBuilder2 source(XContentBuilder source) {
        this.sourceXContentBuilder = source;
        return this;
    }

    public IndexRequestBuilder2 source(String source, XContentType contentType) {
        this.sourceString = source;
        this.sourceContentType = contentType;
        return this;
    }

    public void pipeline(String pipeline) {
        this.pipeline = pipeline;
    }

    public void routing(String routing) {
        this.routing = routing;
    }

    public void refreshPolicy(WriteRequest.RefreshPolicy refreshPolicy) {
        this.refreshPolicy = refreshPolicy;
    }

    @Override
    public IndexRequest buildRequest() {
        IndexRequest indexRequest = new IndexRequest(index);
        try {
            indexRequest.id(id);
            if (sourceMap != null) {
                indexRequest.source(sourceMap);
            }
            if (sourceArray != null) {
                indexRequest.source(sourceArray);
            }
            if (sourceXContentBuilder != null) {
                indexRequest.source(sourceXContentBuilder);
            }
            if (sourceString != null && sourceContentType != null) {
                indexRequest.source(sourceString, sourceContentType);
            }
            if (pipeline != null) {
                indexRequest.setPipeline(pipeline);
            }
            if (routing != null) {
                indexRequest.routing(routing);
            }
            if (refreshPolicy != null) {
                indexRequest.setRefreshPolicy(refreshPolicy);
            }
            return indexRequest;
        } catch (Exception e) {
            indexRequest.decRef();
            throw e;
        }
    }
}
