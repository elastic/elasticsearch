/*
 * Licensed to ElasticSearch and Shay Banon under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. ElasticSearch licenses this
 * file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
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

package org.elasticsearch.action.admin.indices.create;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.master.MasterNodeOperationRequestBuilder;
import org.elasticsearch.client.IndicesAdminClient;
import org.elasticsearch.client.internal.InternalIndicesAdminClient;
import org.elasticsearch.cluster.metadata.IndexMetaData;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.xcontent.XContentBuilder;

import java.util.Map;

/**
 *
 */
public class CreateIndexRequestBuilder extends MasterNodeOperationRequestBuilder<CreateIndexRequest, CreateIndexResponse, CreateIndexRequestBuilder> {

    public CreateIndexRequestBuilder(IndicesAdminClient indicesClient) {
        super((InternalIndicesAdminClient) indicesClient, new CreateIndexRequest());
    }

    public CreateIndexRequestBuilder(IndicesAdminClient indicesClient, String index) {
        super((InternalIndicesAdminClient) indicesClient, new CreateIndexRequest(index));
    }

    public CreateIndexRequestBuilder setIndex(String index) {
        request.setIndex(index);
        return this;
    }

    /**
     * The settings to created the index with.
     */
    public CreateIndexRequestBuilder setSettings(Settings settings) {
        request.setSettings(settings);
        return this;
    }

    /**
     * The settings to created the index with.
     */
    public CreateIndexRequestBuilder setSettings(Settings.Builder settings) {
        request.setSettings(settings);
        return this;
    }

    /**
     * Allows to set the settings using a json builder.
     */
    public CreateIndexRequestBuilder setSettings(XContentBuilder builder) {
        request.setSettings(builder);
        return this;
    }

    /**
     * The settings to crete the index with (either json/yaml/properties format)
     */
    public CreateIndexRequestBuilder setSettings(String source) {
        request.setSettings(source);
        return this;
    }

    /**
     * The settings to crete the index with (either json/yaml/properties format)
     */
    public CreateIndexRequestBuilder setSettings(Map<String, Object> source) {
        request.setSettings(source);
        return this;
    }

    /**
     * Adds mapping that will be added when the index gets created.
     *
     * @param type   The mapping type
     * @param source The mapping source
     */
    public CreateIndexRequestBuilder addMapping(String type, String source) {
        request.addMapping(type, source);
        return this;
    }

    /**
     * The cause for this index creation.
     */
    public CreateIndexRequestBuilder setCause(String cause) {
        request.setCause(cause);
        return this;
    }

    /**
     * Adds mapping that will be added when the index gets created.
     *
     * @param type   The mapping type
     * @param source The mapping source
     */
    public CreateIndexRequestBuilder addMapping(String type, XContentBuilder source) {
        request.addMapping(type, source);
        return this;
    }

    /**
     * Adds mapping that will be added when the index gets created.
     *
     * @param type   The mapping type
     * @param source The mapping source
     */
    public CreateIndexRequestBuilder addMapping(String type, Map<String, Object> source) {
        request.addMapping(type, source);
        return this;
    }

    /**
     * Sets the settings and mappings as a single source.
     */
    public CreateIndexRequestBuilder setSource(String source) {
        request.setSource(source);
        return this;
    }

    /**
     * Sets the settings and mappings as a single source.
     */
    public CreateIndexRequestBuilder setSource(BytesReference source) {
        request.setSource(source);
        return this;
    }

    /**
     * Sets the settings and mappings as a single source.
     */
    public CreateIndexRequestBuilder setSource(byte[] source) {
        request.setSource(source);
        return this;
    }

    /**
     * Sets the settings and mappings as a single source.
     */
    public CreateIndexRequestBuilder setSource(byte[] source, int offset, int length) {
        request.setSource(source, offset, length);
        return this;
    }

    /**
     * Sets the settings and mappings as a single source.
     */
    public CreateIndexRequestBuilder setSource(Map<String, Object> source) {
        request.setSource(source);
        return this;
    }

    public CreateIndexRequestBuilder addCustom(IndexMetaData.Custom custom) {
        request.addCustom(custom);
        return this;
    }

    /**
     * Sets the settings and mappings as a single source.
     */
    public CreateIndexRequestBuilder setSource(XContentBuilder source) {
        request.setSource(source);
        return this;
    }

    /**
     * Timeout to wait for the index creation to be acknowledged by current cluster nodes. Defaults
     * to <tt>10s</tt>.
     */
    public CreateIndexRequestBuilder setTimeout(TimeValue timeout) {
        request.setTimeout(timeout);
        return this;
    }

    /**
     * Timeout to wait for the index creation to be acknowledged by current cluster nodes. Defaults
     * to <tt>10s</tt>.
     */
    public CreateIndexRequestBuilder setTimeout(String timeout) {
        request.setTimeout(timeout);
        return this;
    }

    @Override
    protected void doExecute(ActionListener<CreateIndexResponse> listener) {
        ((IndicesAdminClient) client).create(request, listener);
    }
}
