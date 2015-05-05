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

package org.elasticsearch.action.admin.indices.warmer.delete;

import org.elasticsearch.action.support.IndicesOptions;
import org.elasticsearch.action.support.master.AcknowledgedRequestBuilder;
import org.elasticsearch.client.ElasticsearchClient;

/**
 * A builder for the {@link DeleteWarmerRequest}
 *
 * @see DeleteWarmerRequest for details
 */
public class DeleteWarmerRequestBuilder extends AcknowledgedRequestBuilder<DeleteWarmerRequest, DeleteWarmerResponse, DeleteWarmerRequestBuilder> {

    public DeleteWarmerRequestBuilder(ElasticsearchClient client, DeleteWarmerAction action) {
        super(client, action, new DeleteWarmerRequest());
    }

    public DeleteWarmerRequestBuilder setIndices(String... indices) {
        request.indices(indices);
        return this;
    }

    /**
     * The name (or wildcard expression) of the index warmer to delete, or null
     * to delete all warmers.
     */
    public DeleteWarmerRequestBuilder setNames(String... names) {
        request.names(names);
        return this;
    }

    /**
     * Specifies what type of requested indices to ignore and wildcard indices expressions.
     * <p/>
     * For example indices that don't exist.
     */
    public DeleteWarmerRequestBuilder setIndicesOptions(IndicesOptions options) {
        request.indicesOptions(options);
        return this;
    }
}
