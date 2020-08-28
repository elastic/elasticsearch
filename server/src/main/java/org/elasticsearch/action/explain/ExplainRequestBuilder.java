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

package org.elasticsearch.action.explain;

import org.elasticsearch.action.support.single.shard.SingleShardOperationRequestBuilder;
import org.elasticsearch.client.ElasticsearchClient;
import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.Strings;
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
        FetchSourceContext fetchSourceContext = request.fetchSourceContext() != null ? request.fetchSourceContext()
            : FetchSourceContext.FETCH_SOURCE;
        request.fetchSourceContext(new FetchSourceContext(fetch, fetchSourceContext.includes(), fetchSourceContext.excludes()));
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
                include == null ? Strings.EMPTY_ARRAY : new String[]{include},
                exclude == null ? Strings.EMPTY_ARRAY : new String[]{exclude});
    }

    /**
     * Indicate that _source should be returned, with an "include" and/or "exclude" set which can include simple wildcard
     * elements.
     *
     * @param includes An optional list of include (optionally wildcarded) pattern to filter the returned _source
     * @param excludes An optional list of exclude (optionally wildcarded) pattern to filter the returned _source
     */
    public ExplainRequestBuilder setFetchSource(@Nullable String[] includes, @Nullable String[] excludes) {
        FetchSourceContext fetchSourceContext = request.fetchSourceContext() != null ? request.fetchSourceContext()
            : FetchSourceContext.FETCH_SOURCE;
        request.fetchSourceContext(new FetchSourceContext(fetchSourceContext.fetchSource(), includes, excludes));
        return this;
    }
}
