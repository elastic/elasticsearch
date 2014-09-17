/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.elasticsearch.action.admin.indices.mapping.put;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.IndicesOptions;
import org.elasticsearch.action.support.master.AcknowledgedRequestBuilder;
import org.elasticsearch.client.IndicesAdminClient;
import org.elasticsearch.common.xcontent.XContentBuilder;

import java.util.Map;

/**
 * Builder for a put mapping request
 */
public class PutMappingRequestBuilder extends AcknowledgedRequestBuilder<PutMappingRequest, PutMappingResponse, PutMappingRequestBuilder, IndicesAdminClient> {

    public PutMappingRequestBuilder(IndicesAdminClient indicesClient) {
        super(indicesClient, new PutMappingRequest());
    }

    public PutMappingRequestBuilder setIndices(String... indices) {
        request.indices(indices);
        return this;
    }

    /**
     * Specifies what type of requested indices to ignore and wildcard indices expressions.
     *
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
    public PutMappingRequestBuilder setSource(Map mappingSource) {
        request.source(mappingSource);
        return this;
    }

    /**
     * The mapping source definition.
     */
    public PutMappingRequestBuilder setSource(String mappingSource) {
        request.source(mappingSource);
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

    /**
     * If there is already a mapping definition registered against the type, then it will be merged. If there are
     * elements that can't be merged are detected, the request will be rejected unless the
     * {@link #setIgnoreConflicts(boolean)} is set. In such a case, the duplicate mappings will be rejected.
     */
    public PutMappingRequestBuilder setIgnoreConflicts(boolean ignoreConflicts) {
        request.ignoreConflicts(ignoreConflicts);
        return this;
    }

    @Override
    protected void doExecute(ActionListener<PutMappingResponse> listener) {
        client.putMapping(request, listener);
    }
}
