/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.action.explain;

import org.elasticsearch.action.support.single.shard.SingleShardOperationRequestBuilder;
import org.elasticsearch.client.internal.ElasticsearchClient;
import org.elasticsearch.common.Strings;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.search.fetch.subphase.FetchSourceContext;

/**
 * A builder for {@link ExplainRequest}.
 */
public class ExplainRequestBuilder extends SingleShardOperationRequestBuilder<ExplainRequest, ExplainResponse, ExplainRequestBuilder> {

    ExplainRequestBuilder(ElasticsearchClient client, ExplainAction action) {
        super(client, action, new ExplainRequest());
    }

    public ExplainRequestBuilder(ElasticsearchClient client, ExplainAction action, String index, String id) {
        super(client, action, new ExplainRequest().index(index).id(id));
    }

    /**
     * Sets the id to get a score explanation for.
     */
    public ExplainRequestBuilder setId(String id) {
        request().id(id);
        return this;
    }

    /**
     * Sets the routing for sharding.
     */
    public ExplainRequestBuilder setRouting(String routing) {
        request().routing(routing);
        return this;
    }

    /**
     * Simple sets the routing. Since the parent is only used to get to the right shard.
     */
    public ExplainRequestBuilder setParent(String parent) {
        request().parent(parent);
        return this;
    }

    /**
     * Sets the shard preference.
     */
    public ExplainRequestBuilder setPreference(String preference) {
        request().preference(preference);
        return this;
    }

    /**
     * Sets the query to get a score explanation for.
     */
    public ExplainRequestBuilder setQuery(QueryBuilder query) {
        request.query(query);
        return this;
    }

    /**
     * Explicitly specify the stored fields that will be returned for the explained document. By default, nothing is returned.
     */
    public ExplainRequestBuilder setStoredFields(String... fields) {
        request.storedFields(fields);
        return this;
    }

    /**
     * Indicates whether the response should contain the stored _source
     */
    public ExplainRequestBuilder setFetchSource(boolean fetch) {
        FetchSourceContext fetchSourceContext = request.fetchSourceContext() != null
            ? request.fetchSourceContext()
            : FetchSourceContext.FETCH_SOURCE;
        request.fetchSourceContext(FetchSourceContext.of(fetch, fetchSourceContext.includes(), fetchSourceContext.excludes()));
        return this;
    }

    /**
     * Indicate that _source should be returned, with an "include" and/or "exclude" set which can include simple wildcard
     * elements.
     *
     * @param include An optional include (optionally wildcarded) pattern to filter the returned _source
     * @param exclude An optional exclude (optionally wildcarded) pattern to filter the returned _source
     */
    public ExplainRequestBuilder setFetchSource(@Nullable String include, @Nullable String exclude) {
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
    public ExplainRequestBuilder setFetchSource(@Nullable String[] includes, @Nullable String[] excludes) {
        FetchSourceContext fetchSourceContext = request.fetchSourceContext() != null
            ? request.fetchSourceContext()
            : FetchSourceContext.FETCH_SOURCE;
        request.fetchSourceContext(FetchSourceContext.of(fetchSourceContext.fetchSource(), includes, excludes));
        return this;
    }
}
