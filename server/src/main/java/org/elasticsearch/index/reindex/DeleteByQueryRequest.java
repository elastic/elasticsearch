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

package org.elasticsearch.index.reindex;

import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.action.IndicesRequest;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.support.IndicesOptions;
import org.elasticsearch.tasks.TaskId;

import static org.elasticsearch.action.ValidateActions.addValidationError;

/**
 * Creates a new {@link DeleteByQueryRequest} that uses scrolling and bulk requests to delete all documents matching
 * the query. This can have performance as well as visibility implications.
 *
 * Delete-by-query now has the following semantics:
 * <ul>
 *     <li>it's <tt>non-atomic</tt>, a delete-by-query may fail at any time while some documents matching the query have already been
 *     deleted</li>
 *     <li>it's <tt>syntactic sugar</tt>, a delete-by-query is equivalent to a scroll search and corresponding bulk-deletes by ID</li>
 *     <li>it's executed on a <tt>point-in-time</tt> snapshot, a delete-by-query will only delete the documents that are visible at the
 *     point in time the delete-by-query was started, equivalent to the scroll API</li>
 *     <li>it's <tt>consistent</tt>, a delete-by-query will yield consistent results across all replicas of a shard</li>
 *     <li>it's <tt>forward-compatible</tt>, a delete-by-query will only send IDs to the shards as deletes such that no queries are
 *     stored in the transaction logs that might not be supported in the future.</li>
 *     <li>it's results won't be visible until the index is refreshed.</li>
 * </ul>
 */
public class DeleteByQueryRequest extends AbstractBulkByScrollRequest<DeleteByQueryRequest> implements IndicesRequest.Replaceable {

    public DeleteByQueryRequest() {
    }

    public DeleteByQueryRequest(SearchRequest search) {
        this(search, true);
    }

    private DeleteByQueryRequest(SearchRequest search, boolean setDefaults) {
        super(search, setDefaults);
        // Delete-By-Query does not require the source
        if (setDefaults) {
            search.source().fetchSource(false);
        }
    }

    @Override
    protected DeleteByQueryRequest self() {
        return this;
    }

    @Override
    public ActionRequestValidationException validate() {
        ActionRequestValidationException e = super.validate();
        if (getSearchRequest().indices() == null || getSearchRequest().indices().length == 0) {
            e = addValidationError("use _all if you really want to delete from all existing indexes", e);
        }
        if (getSearchRequest() == null || getSearchRequest().source() == null) {
            e = addValidationError("source is missing", e);
        } else if (getSearchRequest().source().query() == null) {
            e = addValidationError("query is missing", e);
        }
        return e;
    }

    @Override
    public DeleteByQueryRequest forSlice(TaskId slicingTask, SearchRequest slice, int totalSlices) {
        return doForSlice(new DeleteByQueryRequest(slice, false), slicingTask, totalSlices);
    }

    @Override
    public String toString() {
        StringBuilder b = new StringBuilder();
        b.append("delete-by-query ");
        searchToString(b);
        return b.toString();
    }

    //delete by query deletes all documents that match a query. The indices and indices options that affect how
    //indices are resolved depend entirely on the inner search request. That's why the following methods delegate to it.
    @Override
    public IndicesRequest indices(String... indices) {
        assert getSearchRequest() != null;
        getSearchRequest().indices(indices);
        return this;
    }

    @Override
    public String[] indices() {
        assert getSearchRequest() != null;
        return getSearchRequest().indices();
    }

    @Override
    public IndicesOptions indicesOptions() {
        assert getSearchRequest() != null;
        return getSearchRequest().indicesOptions();
    }

    public String[] types() {
        assert getSearchRequest() != null;
        return getSearchRequest().types();
    }

    public DeleteByQueryRequest types(String... types) {
        assert getSearchRequest() != null;
        getSearchRequest().types(types);
        return this;
    }

}
