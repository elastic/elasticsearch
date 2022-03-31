/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.index.reindex;

import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.action.IndicesRequest;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.support.IndicesOptions;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.tasks.TaskId;
import org.elasticsearch.xcontent.ToXContentObject;
import org.elasticsearch.xcontent.XContentBuilder;

import java.io.IOException;

import static org.elasticsearch.action.ValidateActions.addValidationError;

/**
 * Creates a new {@link DeleteByQueryRequest} that uses scrolling and bulk requests to delete all documents matching
 * the query. This can have performance as well as visibility implications.
 *
 * Delete-by-query now has the following semantics:
 * <ul>
 *     <li>it's {@code non-atomic}, a delete-by-query may fail at any time while some documents matching the query have already been
 *     deleted</li>
 *     <li>it's {@code syntactic sugar}, a delete-by-query is equivalent to a scroll search and corresponding bulk-deletes by ID</li>
 *     <li>it's executed on a {@code point-in-time} snapshot, a delete-by-query will only delete the documents that are visible at the
 *     point in time the delete-by-query was started, equivalent to the scroll API</li>
 *     <li>it's {@code consistent}, a delete-by-query will yield consistent results across all replicas of a shard</li>
 *     <li>it's {@code forward-compatible}, a delete-by-query will only send IDs to the shards as deletes such that no queries are
 *     stored in the transaction logs that might not be supported in the future.</li>
 *     <li>it's results won't be visible until the index is refreshed.</li>
 * </ul>
 */
public class DeleteByQueryRequest extends AbstractBulkByScrollRequest<DeleteByQueryRequest>
    implements
        IndicesRequest.Replaceable,
        ToXContentObject {

    public DeleteByQueryRequest() {
        this(new SearchRequest());
    }

    public DeleteByQueryRequest(String... indices) {
        this(new SearchRequest(indices));
    }

    DeleteByQueryRequest(SearchRequest search) {
        this(search, true);
    }

    public DeleteByQueryRequest(StreamInput in) throws IOException {
        super(in);
    }

    private DeleteByQueryRequest(SearchRequest search, boolean setDefaults) {
        super(search, setDefaults);
        // Delete-By-Query does not require the source
        if (setDefaults) {
            search.source().fetchSource(false);
        }
    }

    /**
     * Set the query for selective delete
     */
    public DeleteByQueryRequest setQuery(QueryBuilder query) {
        if (query != null) {
            getSearchRequest().source().query(query);
        }
        return this;
    }

    /**
     * Set routing limiting the process to the shards that match that routing value
     */
    public DeleteByQueryRequest setRouting(String routing) {
        if (routing != null) {
            getSearchRequest().routing(routing);
        }
        return this;
    }

    /**
     * The scroll size to control number of documents processed per batch
     */
    public DeleteByQueryRequest setBatchSize(int size) {
        getSearchRequest().source().size(size);
        return this;
    }

    /**
     * Set the IndicesOptions for controlling unavailable indices
     */
    public DeleteByQueryRequest setIndicesOptions(IndicesOptions indicesOptions) {
        getSearchRequest().indicesOptions(indicesOptions);
        return this;
    }

    /**
     * Gets the batch size for this request
     */
    public int getBatchSize() {
        return getSearchRequest().source().size();
    }

    /**
     * Gets the routing value used for this request
     */
    public String getRouting() {
        return getSearchRequest().routing();
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

    // delete by query deletes all documents that match a query. The indices and indices options that affect how
    // indices are resolved depend entirely on the inner search request. That's why the following methods delegate to it.
    @Override
    public DeleteByQueryRequest indices(String... indices) {
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

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        getSearchRequest().source().innerToXContent(builder, params);
        builder.endObject();
        return builder;
    }
}
