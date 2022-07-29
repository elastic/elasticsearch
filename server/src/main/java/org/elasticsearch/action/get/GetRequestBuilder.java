/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.action.get;

import org.elasticsearch.action.support.single.shard.SingleShardOperationRequestBuilder;
import org.elasticsearch.client.internal.ElasticsearchClient;
import org.elasticsearch.common.Strings;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.index.VersionType;
import org.elasticsearch.search.fetch.subphase.FetchSourceContext;

/**
 * A get document action request builder.
 */
public class GetRequestBuilder extends SingleShardOperationRequestBuilder<GetRequest, GetResponse, GetRequestBuilder> {

    public GetRequestBuilder(ElasticsearchClient client, GetAction action) {
        super(client, action, new GetRequest());
    }

    public GetRequestBuilder(ElasticsearchClient client, GetAction action, @Nullable String index) {
        super(client, action, new GetRequest(index));
    }

    /**
     * Sets the id of the document to fetch.
     */
    public GetRequestBuilder setId(String id) {
        request.id(id);
        return this;
    }

    /**
     * Controls the shard routing of the request. Using this value to hash the shard
     * and not the id.
     */
    public GetRequestBuilder setRouting(String routing) {
        request.routing(routing);
        return this;
    }

    /**
     * Sets the preference to execute the search. Defaults to randomize across shards. Can be set to
     * {@code _local} to prefer local shards or a custom value, which guarantees that the same order
     * will be used across different requests.
     */
    public GetRequestBuilder setPreference(String preference) {
        request.preference(preference);
        return this;
    }

    /**
     * Explicitly specify the fields that will be returned. By default, the {@code _source}
     * field will be returned.
     */
    public GetRequestBuilder setStoredFields(String... fields) {
        request.storedFields(fields);
        return this;
    }

    /**
     * Indicates whether the response should contain the stored _source.
     *
     * @return this for chaining
     */
    public GetRequestBuilder setFetchSource(boolean fetch) {
        FetchSourceContext context = request.fetchSourceContext() == null ? FetchSourceContext.FETCH_SOURCE : request.fetchSourceContext();
        request.fetchSourceContext(FetchSourceContext.of(fetch, context.includes(), context.excludes()));
        return this;
    }

    /**
     * Indicate that _source should be returned, with an "include" and/or "exclude" set which can include simple wildcard
     * elements.
     *
     * @param include An optional include (optionally wildcarded) pattern to filter the returned _source
     * @param exclude An optional exclude (optionally wildcarded) pattern to filter the returned _source
     */
    public GetRequestBuilder setFetchSource(@Nullable String include, @Nullable String exclude) {
        return setFetchSource(
            include == null ? Strings.EMPTY_ARRAY : new String[] { include },
            exclude == null ? Strings.EMPTY_ARRAY : new String[] { exclude }
        );
    }

    /**
     * Indicate that _source should be returned, with an "include" and/or "exclude" set which can include simple wildcard
     * elements.
     *
     * @param includes An optional list of include (optionally wildcarded) pattern to filter the returned _source
     * @param excludes An optional list of exclude (optionally wildcarded) pattern to filter the returned _source
     */
    public GetRequestBuilder setFetchSource(@Nullable String[] includes, @Nullable String[] excludes) {
        FetchSourceContext context = request.fetchSourceContext() == null ? FetchSourceContext.FETCH_SOURCE : request.fetchSourceContext();
        request.fetchSourceContext(FetchSourceContext.of(context.fetchSource(), includes, excludes));
        return this;
    }

    /**
     * Should a refresh be executed before this get operation causing the operation to
     * return the latest value. Note, heavy get should not set this to {@code true}. Defaults
     * to {@code false}.
     */
    public GetRequestBuilder setRefresh(boolean refresh) {
        request.refresh(refresh);
        return this;
    }

    public GetRequestBuilder setRealtime(boolean realtime) {
        request.realtime(realtime);
        return this;
    }

    /**
     * Sets the version, which will cause the get operation to only be performed if a matching
     * version exists and no changes happened on the doc since then.
     */
    public GetRequestBuilder setVersion(long version) {
        request.version(version);
        return this;
    }

    /**
     * Sets the versioning type. Defaults to {@link org.elasticsearch.index.VersionType#INTERNAL}.
     */
    public GetRequestBuilder setVersionType(VersionType versionType) {
        request.versionType(versionType);
        return this;
    }
}
