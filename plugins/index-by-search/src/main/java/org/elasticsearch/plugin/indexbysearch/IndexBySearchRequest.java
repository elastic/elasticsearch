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

package org.elasticsearch.plugin.indexbysearch;

import static org.elasticsearch.action.ValidateActions.addValidationError;

import java.io.IOException;

import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexRequest.OpType;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.lucene.uid.Versions;
import org.elasticsearch.index.VersionType;

public class IndexBySearchRequest extends AbstractBulkByScrollRequest<IndexBySearchRequest> {
    /**
     * Prototype for index requests.
     *
     * Note that we co-opt version = Versions.NOT_SET to mean
     * "do not set the version in the index requests that we send for each scroll hit."
     */
    private IndexRequest index;

    public IndexBySearchRequest() {
    }

    public IndexBySearchRequest(SearchRequest search, IndexRequest index) {
        super(search);
        this.index = index;

        // Clear the versionType so we can check if we've parsed it
        index.versionType(null);
    }

    @Override
    protected IndexBySearchRequest self() {
        return this;
    }

    @Override
    public ActionRequestValidationException validate() {
        ActionRequestValidationException e = super.validate();
        /*
         * Note that we don't validate the index here - it won't work because
         * we'll be filling in portions of it as we receive the docs. But we can
         * validate some things.
         */
        if (index.index() == null) {
            e = addValidationError("index must be specified", e);
        }
        if (false == (index.routing() == null || index.routing().startsWith("=") ||
                "keep".equals(index.routing()) || "discard".equals(index.routing()))) {
            e = addValidationError("routing must be unset, [keep], [discard] or [=<some new value>]", e);
        }
        return e;
    }

    public IndexRequest index() {
        return index;
    }

    @Override
    public void fillInConditionalDefaults() {
        super.fillInConditionalDefaults();
        if (index().versionType() == null) {
            setupDefaultVersionType();
        }
    }

    void setupDefaultVersionType() {
        if (index().version() == Versions.NOT_SET) {
            /*
             * Not set means just don't set it on the index request. That
             * doesn't work properly with some VersionTypes so lets just set it
             * to something simple.
             */
            index().versionType(VersionType.INTERNAL);
            return;
        }
        if (destinationSameAsSource()) {
            // Only writes on versions == and writes the version number
            index().versionType(VersionType.REINDEX);
            return;
        }
        index().opType(OpType.CREATE);
    }

    /**
     * Are the source and the destination "the same". Useful for conditional defaults. The rules are:
     * <ul>
     *  <li>Is the source exactly one index? No === false<li>
     *  <li>Is the single source index the same as the destination index? No === false</li>
     *  <li>Is the destination type null? Yes === true if the source type is also empty, false otherwise</li>
     *  <li>Is the source exactly one type? No === false</li>
     *  <li>true if the single source type is the same as the destination type</li>
     * </ul>
     */
    public boolean destinationSameAsSource() {
        if (search().indices() == null || search().indices().length != 1) {
            return false;
        }
        if (false == search().indices()[0].equals(index.index())) {
            return false;
        }
        if (index.type() == null) {
            return search().types() == null || search().types().length == 0;
        }
        if (search().types().length != 1) {
            return false;
        }
        return search().types()[0].equals(index.type());
    }

    @Override
    public void readFrom(StreamInput in) throws IOException {
        super.readFrom(in);
        index.readFrom(in);
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        index.writeTo(out);
    }

    @Override
    public String toString() {
        StringBuilder b = new StringBuilder();
        b.append("index-by-search from ");
        searchToString(b);
        b.append(" to [").append(index.index()).append(']');
        if (index.type() != null) {
            b.append('[').append(index.type()).append(']');
        }
        return b.toString();
    }

    /**
     * Parse and set the version on an index request. This is used to handle
     * index-by-search specific parsing logic for versions.
     */
    public static void setVersionOnIndexRequest(IndexRequest indexRequest, String version) {
        switch (version) {
        case "not_set":
            indexRequest.version(Versions.NOT_SET);
            return;
        default:
            throw new IllegalArgumentException("Invalid version: " + version);
        }
    }
}
