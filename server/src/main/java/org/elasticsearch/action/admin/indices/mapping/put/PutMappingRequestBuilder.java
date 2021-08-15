/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.action.admin.indices.mapping.put;

import org.elasticsearch.action.support.IndicesOptions;
import org.elasticsearch.action.support.master.AcknowledgedRequestBuilder;
import org.elasticsearch.action.support.master.AcknowledgedResponse;
import org.elasticsearch.client.ElasticsearchClient;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.index.Index;

import java.util.Map;

/**
 * Builder for a put mapping request
 */
public class PutMappingRequestBuilder
    extends AcknowledgedRequestBuilder<PutMappingRequest, AcknowledgedResponse, PutMappingRequestBuilder> {

    public PutMappingRequestBuilder(ElasticsearchClient client, PutMappingAction action) {
        super(client, action, new PutMappingRequest());
    }

    public PutMappingRequestBuilder setIndices(String... indices) {
        request.indices(indices);
        return this;
    }

    public PutMappingRequestBuilder setConcreteIndex(Index index) {
        request.setConcreteIndex(index);
        return this;
    }

    /**
     * Specifies what type of requested indices to ignore and wildcard indices expressions.
     * <p>
     * For example indices that don't exist.
     */
    public PutMappingRequestBuilder setIndicesOptions(IndicesOptions options) {
        request.indicesOptions(options);
        return this;
    }

    /**
     * The type of the mappings.
     */
    public PutMappingRequestBuilder setType(String type) {
        request.type(type);
        return this;
    }

    /**
     * The mapping source definition.
     */
    public PutMappingRequestBuilder setSource(XContentBuilder mappingBuilder) {
        request.source(mappingBuilder);
        return this;
    }

    /**
     * The mapping source definition.
     */
    public PutMappingRequestBuilder setSource(Map<String, ?> mappingSource) {
        request.source(mappingSource);
        return this;
    }

    /**
     * The mapping source definition.
     */
    public PutMappingRequestBuilder setSource(String mappingSource, XContentType xContentType) {
        request.source(mappingSource, xContentType);
        return this;
    }

    /**
     * A specialized simplified mapping source method, takes the form of simple properties definition:
     * ("field1", "type=string,store=true").
     */
    public PutMappingRequestBuilder setSource(Object... source) {
        request.source(source);
        return this;
    }

}
