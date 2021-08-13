/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.action.admin.indices.create;

import org.elasticsearch.action.admin.indices.alias.Alias;
import org.elasticsearch.action.support.ActiveShardCount;
import org.elasticsearch.action.support.master.AcknowledgedRequestBuilder;
import org.elasticsearch.client.ElasticsearchClient;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.LoggingDeprecationHandler;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentType;

import java.util.Map;

/**
 * Builder for a create index request
 */
public class CreateIndexRequestBuilder
    extends AcknowledgedRequestBuilder<CreateIndexRequest, CreateIndexResponse, CreateIndexRequestBuilder> {

    public CreateIndexRequestBuilder(ElasticsearchClient client, CreateIndexAction action) {
        super(client, action, new CreateIndexRequest());
    }

    public CreateIndexRequestBuilder(ElasticsearchClient client, CreateIndexAction action, String index) {
        super(client, action, new CreateIndexRequest(index));
    }

    /**
     * Sets the name of the index to be created
     */
    public CreateIndexRequestBuilder setIndex(String index) {
        request.index(index);
        return this;
    }

    /**
     * The settings to create the index with.
     */
    public CreateIndexRequestBuilder setSettings(Settings settings) {
        request.settings(settings);
        return this;
    }

    /**
     * The settings to create the index with.
     */
    public CreateIndexRequestBuilder setSettings(Settings.Builder settings) {
        request.settings(settings);
        return this;
    }

    /**
     * Allows to set the settings using a json builder.
     */
    public CreateIndexRequestBuilder setSettings(XContentBuilder builder) {
        request.settings(builder);
        return this;
    }

    /**
     * The settings to create the index with (either json or yaml format)
     */
    public CreateIndexRequestBuilder setSettings(String source, XContentType xContentType) {
        request.settings(source, xContentType);
        return this;
    }

    /**
     * The settings to create the index with (either json/yaml/properties format)
     */
    public CreateIndexRequestBuilder setSettings(Map<String, ?> source) {
        request.settings(source);
        return this;
    }

    /**
     * Adds mapping that will be added when the index gets created.
     *
     * @param source The mapping source
     */
    public CreateIndexRequestBuilder setMapping(String source) {
        request.mapping(source);
        return this;
    }

    /**
     * The cause for this index creation.
     */
    public CreateIndexRequestBuilder setCause(String cause) {
        request.cause(cause);
        return this;
    }

    /**
     * Adds mapping that will be added when the index gets created.
     *
     * @param source The mapping source
     */
    public CreateIndexRequestBuilder setMapping(XContentBuilder source) {
        request.mapping(source);
        return this;
    }

    /**
     * Adds mapping that will be added when the index gets created.
     *
     * @param source The mapping source
     */
    public CreateIndexRequestBuilder setMapping(Map<String, Object> source) {
        request.mapping(source);
        return this;
    }

    /**
     * A specialized simplified mapping source method, takes the form of simple properties definition:
     * ("field1", "type=string,store=true").
     */
    public CreateIndexRequestBuilder setMapping(String... source) {
        request.simpleMapping(source);
        return this;
    }

    /**
     * Sets the aliases that will be associated with the index when it gets created
     */
    public CreateIndexRequestBuilder setAliases(Map<String, ?> source) {
        request.aliases(source);
        return this;
    }

    /**
     * Sets the aliases that will be associated with the index when it gets created
     */
    public CreateIndexRequestBuilder setAliases(String source) {
        request.aliases(source);
        return this;
    }

    /**
     * Sets the aliases that will be associated with the index when it gets created
     */
    public CreateIndexRequestBuilder setAliases(XContentBuilder source) {
        request.aliases(source);
        return this;
    }

    /**
     * Sets the aliases that will be associated with the index when it gets created
     */
    public CreateIndexRequestBuilder setAliases(BytesReference source) {
        request.aliases(source);
        return this;
    }

    /**
     * Adds an alias that will be associated with the index when it gets created
     */
    public CreateIndexRequestBuilder addAlias(Alias alias) {
        request.alias(alias);
        return this;
    }

    /**
     * Sets the settings and mappings as a single source.
     */
    public CreateIndexRequestBuilder setSource(String source, XContentType xContentType) {
        request.source(source, xContentType);
        return this;
    }

    /**
     * Sets the settings and mappings as a single source.
     */
    public CreateIndexRequestBuilder setSource(BytesReference source, XContentType xContentType) {
        request.source(source, xContentType);
        return this;
    }

    /**
     * Sets the settings and mappings as a single source.
     */
    public CreateIndexRequestBuilder setSource(byte[] source, XContentType xContentType) {
        request.source(source, xContentType);
        return this;
    }

    /**
     * Sets the settings and mappings as a single source.
     */
    public CreateIndexRequestBuilder setSource(byte[] source, int offset, int length, XContentType xContentType) {
        request.source(source, offset, length, xContentType);
        return this;
    }

    /**
     * Sets the settings and mappings as a single source.
     */
    public CreateIndexRequestBuilder setSource(Map<String, ?> source) {
        request.source(source, LoggingDeprecationHandler.INSTANCE);
        return this;
    }

    /**
     * Sets the settings and mappings as a single source.
     */
    public CreateIndexRequestBuilder setSource(XContentBuilder source) {
        request.source(source);
        return this;
    }

    /**
     * Sets the number of shard copies that should be active for index creation to return.
     * Defaults to {@link ActiveShardCount#DEFAULT}, which will wait for one shard copy
     * (the primary) to become active. Set this value to {@link ActiveShardCount#ALL} to
     * wait for all shards (primary and all replicas) to be active before returning.
     * Otherwise, use {@link ActiveShardCount#from(int)} to set this value to any
     * non-negative integer, up to the number of copies per shard (number of replicas + 1),
     * to wait for the desired amount of shard copies to become active before returning.
     * Index creation will only wait up until the timeout value for the number of shard copies
     * to be active before returning.  Check {@link CreateIndexResponse#isShardsAcknowledged()} to
     * determine if the requisite shard copies were all started before returning or timing out.
     *
     * @param waitForActiveShards number of active shard copies to wait on
     */
    public CreateIndexRequestBuilder setWaitForActiveShards(ActiveShardCount waitForActiveShards) {
        request.waitForActiveShards(waitForActiveShards);
        return this;
    }

    /**
     * A shortcut for {@link #setWaitForActiveShards(ActiveShardCount)} where the numerical
     * shard count is passed in, instead of having to first call {@link ActiveShardCount#from(int)}
     * to get the ActiveShardCount.
     */
    public CreateIndexRequestBuilder setWaitForActiveShards(final int waitForActiveShards) {
        return setWaitForActiveShards(ActiveShardCount.from(waitForActiveShards));
    }
}
