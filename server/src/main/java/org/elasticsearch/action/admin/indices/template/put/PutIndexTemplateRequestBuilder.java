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
package org.elasticsearch.action.admin.indices.template.put;

import org.elasticsearch.action.admin.indices.alias.Alias;
import org.elasticsearch.action.support.master.AcknowledgedResponse;
import org.elasticsearch.action.support.master.MasterNodeOperationRequestBuilder;
import org.elasticsearch.client.ElasticsearchClient;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentType;

import java.util.List;
import java.util.Map;

public class PutIndexTemplateRequestBuilder
    extends MasterNodeOperationRequestBuilder<PutIndexTemplateRequest, AcknowledgedResponse, PutIndexTemplateRequestBuilder> {

    public PutIndexTemplateRequestBuilder(ElasticsearchClient client, PutIndexTemplateAction action) {
        super(client, action, new PutIndexTemplateRequest());
    }

    public PutIndexTemplateRequestBuilder(ElasticsearchClient client, PutIndexTemplateAction action, String name) {
        super(client, action, new PutIndexTemplateRequest(name));
    }

    /**
     * Sets the match expression that will be used to match on indices created.
     */
    public PutIndexTemplateRequestBuilder setPatterns(List<String> indexPatterns) {
        request.patterns(indexPatterns);
        return this;
    }

    /**
     * Sets the order of this template if more than one template matches.
     */
    public PutIndexTemplateRequestBuilder setOrder(int order) {
        request.order(order);
        return this;
    }

    /**
     * Sets the optional version of this template.
     */
    public PutIndexTemplateRequestBuilder setVersion(Integer version) {
        request.version(version);
        return this;
    }

    /**
     * Set to {@code true} to force only creation, not an update of an index template. If it already
     * exists, it will fail with an {@link IllegalArgumentException}.
     */
    public PutIndexTemplateRequestBuilder setCreate(boolean create) {
        request.create(create);
        return this;
    }

    /**
     * The settings to created the index template with.
     */
    public PutIndexTemplateRequestBuilder setSettings(Settings settings) {
        request.settings(settings);
        return this;
    }

    /**
     * The settings to created the index template with.
     */
    public PutIndexTemplateRequestBuilder setSettings(Settings.Builder settings) {
        request.settings(settings);
        return this;
    }

    /**
     * The settings to crete the index template with (either json or yaml format)
     */
    public PutIndexTemplateRequestBuilder setSettings(String source, XContentType xContentType) {
        request.settings(source, xContentType);
        return this;
    }

    /**
     * The settings to crete the index template with (either json or yaml format)
     */
    public PutIndexTemplateRequestBuilder setSettings(Map<String, Object> source) {
        request.settings(source);
        return this;
    }

    /**
     * Adds mapping that will be added when the index template gets created.
     *
     * @param type   The mapping type
     * @param source The mapping source
     * @param xContentType The type/format of the source
     */
    public PutIndexTemplateRequestBuilder addMapping(String type, String source, XContentType xContentType) {
        request.mapping(type, source, xContentType);
        return this;
    }

    /**
     * A specialized simplified mapping source method, takes the form of simple properties definition:
     * ("field1", "type=string,store=true").
     */
    public PutIndexTemplateRequestBuilder addMapping(String type, Object... source) {
        request.mapping(type, source);
        return this;
    }

    /**
     * Sets the aliases that will be associated with the index when it gets created
     */
    public PutIndexTemplateRequestBuilder setAliases(Map<String, Object> source) {
        request.aliases(source);
        return this;
    }

    /**
     * Sets the aliases that will be associated with the index when it gets created
     */
    public PutIndexTemplateRequestBuilder setAliases(String source) {
        request.aliases(source);
        return this;
    }

    /**
     * Sets the aliases that will be associated with the index when it gets created
     */
    public PutIndexTemplateRequestBuilder setAliases(XContentBuilder source) {
        request.aliases(source);
        return this;
    }

    /**
     * Sets the aliases that will be associated with the index when it gets created
     */
    public PutIndexTemplateRequestBuilder setAliases(BytesReference source) {
        request.aliases(source);
        return this;
    }

    /**
     * Adds an alias that will be added when the index template gets created.
     *
     * @param alias The alias
     * @return the request builder
     */
    public PutIndexTemplateRequestBuilder addAlias(Alias alias) {
        request.alias(alias);
        return this;
    }

    /**
     * The cause for this index template creation.
     */
    public PutIndexTemplateRequestBuilder cause(String cause) {
        request.cause(cause);
        return this;
    }

    /**
     * Adds mapping that will be added when the index template gets created.
     *
     * @param type   The mapping type
     * @param source The mapping source
     */
    public PutIndexTemplateRequestBuilder addMapping(String type, XContentBuilder source) {
        request.mapping(type, source);
        return this;
    }

    /**
     * Adds mapping that will be added when the index gets created.
     *
     * @param type   The mapping type
     * @param source The mapping source
     */
    public PutIndexTemplateRequestBuilder addMapping(String type, Map<String, Object> source) {
        request.mapping(type, source);
        return this;
    }

    /**
     * The template source definition.
     */
    public PutIndexTemplateRequestBuilder setSource(XContentBuilder templateBuilder) {
        request.source(templateBuilder);
        return this;
    }

    /**
     * The template source definition.
     */
    public PutIndexTemplateRequestBuilder setSource(Map<String, Object> templateSource) {
        request.source(templateSource);
        return this;
    }

    /**
     * The template source definition.
     */
    public PutIndexTemplateRequestBuilder setSource(BytesReference templateSource, XContentType xContentType) {
        request.source(templateSource, xContentType);
        return this;
    }

    /**
     * The template source definition.
     */
    public PutIndexTemplateRequestBuilder setSource(byte[] templateSource, XContentType xContentType) {
        request.source(templateSource, xContentType);
        return this;
    }

    /**
     * The template source definition.
     */
    public PutIndexTemplateRequestBuilder setSource(byte[] templateSource, int offset, int length, XContentType xContentType) {
        request.source(templateSource, offset, length, xContentType);
        return this;
    }
}
