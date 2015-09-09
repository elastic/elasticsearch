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

package org.elasticsearch.action.deletebyquery;

import org.elasticsearch.ElasticsearchGenerationException;
import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.action.IndicesRequest;
import org.elasticsearch.action.support.IndicesOptions;
import org.elasticsearch.action.support.QuerySourceBuilder;
import org.elasticsearch.client.Requests;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.search.Scroll;

import java.io.IOException;
import java.util.Arrays;
import java.util.Map;

import static org.elasticsearch.action.ValidateActions.addValidationError;
import static org.elasticsearch.search.Scroll.readScroll;

/**
 * Creates a new {@link DeleteByQueryRequest}. Delete-by-query is since elasticsearch 2.0.0 moved into a plugin
 * and is not part of elasticsearch core. In contrast to the previous, in-core, implementation delete-by-query now
 * uses scan/scroll and the returned IDs do delete all documents matching the query. This can have performance
 * as well as visibility implications. Delete-by-query now has the following semantics:
 * <li>
 *     <ul>it's <tt>non-actomic</tt>, a delete-by-query may fail at any time while some documents matching the query have already been deleted</ul>
 *     <ul>it's <tt>try-once</tt>, a delete-by-query may fail at any time and will not retry it's execution. All retry logic is left to the user</ul>
 *     <ul>it's <tt>syntactic sugar</tt>, a delete-by-query is equivalent to a scan/scroll search and corresponding bulk-deletes by ID</ul>
 *     <ul>it's executed on a <tt>point-in-time</tt> snapshot, a delete-by-query will only delete the documents that are visible at the point in time the delete-by-query was started, equivalent to the scan/scroll API</ul>
 *     <ul>it's <tt>consistent</tt>, a delete-by-query will yield consistent results across all replicas of a shard</ul>
 *     <ul>it's <tt>forward-compativle</tt>, a delete-by-query will only send IDs to the shards as deletes such that no queries are stored in the transaction logs that might not be supported in the future.</ul>
 *     <ul>it's results won't be visible until the user refreshes the index.</ul>
 * </li>
 *
 * The main reason why delete-by-query is now extracted as a plugin are:
 *  <li>
 *     <ul><tt>forward-compatibility</tt>, the previous implementation was prone to store unsupported queries in the transaction logs which is equvalent to data-loss</ul>
 *     <ul><tt>consistency & correctness</tt>, the previous implementation was prone to produce different results on a shards replica which can essentially result in a corrupted index</ul>
 *     <ul><tt>resiliency</tt>, the previous implementation could cause OOM errors, merge-storms and dramatic slowdowns if used incorrectly</ul>
 * </li>
 *
 * While delete-by-query is a very useful feature, it's implementation is very tricky in system that is based on per-document modifications. The move towards
 * a plugin based solution was mainly done to minimize the risk of cluster failures or corrupted indices which where easily possible wiht the previous implementation.
 * Users that rely delete by query should install the plugin in oder to use this functionality.
 */
public class DeleteByQueryRequest extends ActionRequest<DeleteByQueryRequest> implements IndicesRequest.Replaceable {

    private String[] indices = Strings.EMPTY_ARRAY;
    private IndicesOptions indicesOptions = IndicesOptions.fromOptions(false, false, true, false);

    private String[] types = Strings.EMPTY_ARRAY;

    private BytesReference source;

    private String routing;

    private int size = 0;

    private Scroll scroll = new Scroll(TimeValue.timeValueMinutes(10));

    private TimeValue timeout;

    public DeleteByQueryRequest() {
    }

    /**
     * Constructs a new delete by query request to run against the provided indices. No indices means
     * it will run against all indices.
     */
    public DeleteByQueryRequest(String... indices) {
        this.indices = indices;
    }

    @Override
    public ActionRequestValidationException validate() {
        ActionRequestValidationException validationException = null;
        if (source == null) {
            validationException = addValidationError("source is missing", validationException);
        }
        return validationException;
    }

    @Override
    public String[] indices() {
        return this.indices;
    }

    @Override
    public DeleteByQueryRequest indices(String[] indices) {
        this.indices = indices;
        return this;
    }

    @Override
    public IndicesOptions indicesOptions() {
        return indicesOptions;
    }

    public DeleteByQueryRequest indicesOptions(IndicesOptions indicesOptions) {
        if (indicesOptions == null) {
            throw new IllegalArgumentException("IndicesOptions must not be null");
        }
        this.indicesOptions = indicesOptions;
        return this;
    }

    public String[] types() {
        return this.types;
    }

    public DeleteByQueryRequest types(String... types) {
        this.types = types;
        return this;
    }

    public BytesReference source() {
        return source;
    }

    public DeleteByQueryRequest source(QuerySourceBuilder sourceBuilder) {
        this.source = sourceBuilder.buildAsBytes(Requests.CONTENT_TYPE);
        return this;
    }

    public DeleteByQueryRequest source(Map<String,?> querySource) {
        try {
            XContentBuilder builder = XContentFactory.contentBuilder(Requests.CONTENT_TYPE);
            builder.map(querySource);
            return source(builder);
        } catch (IOException e) {
            throw new ElasticsearchGenerationException("Failed to generate [" + querySource + "]", e);
        }
    }

    public DeleteByQueryRequest source(XContentBuilder builder) {
        this.source = builder.bytes();
        return this;
    }

    public DeleteByQueryRequest source(String querySource) {
        this.source = new BytesArray(querySource);
        return this;
    }

    public DeleteByQueryRequest source(byte[] querySource) {
        return source(querySource, 0, querySource.length);
    }

    public DeleteByQueryRequest source(byte[] querySource, int offset, int length) {
        return source(new BytesArray(querySource, offset, length));
    }

    public DeleteByQueryRequest source(BytesReference querySource) {
        this.source = querySource;
        return this;
    }

    public String routing() {
        return this.routing;
    }

    public DeleteByQueryRequest routing(String routing) {
        this.routing = routing;
        return this;
    }

    public DeleteByQueryRequest routing(String... routings) {
        this.routing = Strings.arrayToCommaDelimitedString(routings);
        return this;
    }

    public DeleteByQueryRequest size(int size) {
        if (size < 0) {
            throw new IllegalArgumentException("size must be greater than zero");
        }
        this.size = size;
        return this;
    }

    public int size() {
        return size;
    }


    public Scroll scroll() {
        return scroll;
    }

    public DeleteByQueryRequest scroll(Scroll scroll) {
        this.scroll = scroll;
        return this;
    }

    public DeleteByQueryRequest scroll(TimeValue keepAlive) {
        return scroll(new Scroll(keepAlive));
    }

    public DeleteByQueryRequest scroll(String keepAlive) {
        return scroll(new Scroll(TimeValue.parseTimeValue(keepAlive, null, getClass().getSimpleName() + ".keepAlive")));
    }

    public TimeValue timeout() {
        return timeout;
    }

    public DeleteByQueryRequest timeout(TimeValue timeout) {
        if (timeout == null) {
            throw new IllegalArgumentException("timeout must not be null");
        }
        this.timeout = timeout;
        return this;
    }

    public DeleteByQueryRequest timeout(String timeout) {
        timeout(TimeValue.parseTimeValue(timeout, null, getClass().getSimpleName() + ".timeout"));
        return this;
    }

    @Override
    public void readFrom(StreamInput in) throws IOException {
        super.readFrom(in);
        indices = in.readStringArray();
        indicesOptions = IndicesOptions.readIndicesOptions(in);
        types = in.readStringArray();
        source = in.readBytesReference();
        routing = in.readOptionalString();
        size = in.readVInt();
        if (in.readBoolean()) {
            scroll = readScroll(in);
        }
        if (in.readBoolean()) {
            timeout = TimeValue.readTimeValue(in);
        }
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        out.writeStringArray(indices);
        indicesOptions.writeIndicesOptions(out);
        out.writeStringArray(types);
        out.writeBytesReference(source);
        out.writeOptionalString(routing);
        out.writeVInt(size);
        out.writeOptionalStreamable(scroll);
        out.writeOptionalStreamable(timeout);
    }

    @Override
    public String toString() {
        String sSource = "_na_";
        try {
            sSource = XContentHelper.convertToJson(source, false);
        } catch (Exception e) {
            // ignore
        }
        return "delete-by-query [" + Arrays.toString(indices) + "][" + Arrays.toString(types) + "], source[" + sSource + "]";
    }
}
