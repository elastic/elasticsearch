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

package org.elasticsearch.action.update;

import com.google.common.collect.ImmutableList;
import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.index.get.GetResult;
import org.elasticsearch.index.percolator.PercolatorExecutor;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 */
public class UpdateResponse extends ActionResponse {

    private String index;
    private String id;
    private String type;
    private long version;
    private List<PercolatorExecutor.PercolationMatch> matches;
    private GetResult getResult;

    public UpdateResponse() {

    }

    public UpdateResponse(String index, String type, String id, long version) {
        this.index = index;
        this.id = id;
        this.type = type;
        this.version = version;
    }

    /**
     * The index the document was indexed into.
     */
    public String getIndex() {
        return this.index;
    }

    /**
     * The type of the document indexed.
     */
    public String getType() {
        return this.type;
    }

    /**
     * The id of the document indexed.
     */
    public String getId() {
        return this.id;
    }

    /**
     * Returns the version of the doc indexed.
     */
    public long getVersion() {
        return this.version;
    }

    /**
     * Returns the percolate queries matches. <tt>null</tt> if no percolation was requested.
     */
    public List<PercolatorExecutor.PercolationMatch> getMatches() {
        return this.matches;
    }

    void setGetResult(GetResult getResult) {
        this.getResult = getResult;
    }

    public GetResult getGetResult() {
        return this.getResult;
    }

    /**
     * Internal.
     */
    public void setMatches(List<PercolatorExecutor.PercolationMatch> matches) {
        this.matches = matches;
    }

    @Override
    public void readFrom(StreamInput in) throws IOException {
        super.readFrom(in);
        index = in.readString();
        id = in.readString();
        type = in.readString();
        version = in.readLong();
        if (in.readBoolean()) {
            int size = in.readVInt();
            if (size == 0) {
                matches = ImmutableList.of();
            } else if (size == 1) {
                matches = ImmutableList.of(PercolatorExecutor.PercolationMatch.readPercolationMatch(in));
            } else if (size == 2) {
                matches = ImmutableList.of(PercolatorExecutor.PercolationMatch.readPercolationMatch(in), PercolatorExecutor.PercolationMatch.readPercolationMatch(in));
            } else if (size == 3) {
                matches = ImmutableList.of(PercolatorExecutor.PercolationMatch.readPercolationMatch(in), PercolatorExecutor.PercolationMatch.readPercolationMatch(in),
                        PercolatorExecutor.PercolationMatch.readPercolationMatch(in));
            } else {
                matches = new ArrayList<PercolatorExecutor.PercolationMatch>();
                for (int i = 0; i < size; i++) {
                    matches.add(PercolatorExecutor.PercolationMatch.readPercolationMatch(in));
                }
            }
        }
        if (in.readBoolean()) {
            getResult = GetResult.readGetResult(in);
        }
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        out.writeString(index);
        out.writeString(id);
        out.writeString(type);
        out.writeLong(version);
        if (matches == null) {
            out.writeBoolean(false);
        } else {
            out.writeBoolean(true);
            out.writeVInt(matches.size());
            for (PercolatorExecutor.PercolationMatch match : matches) {
                match.writeTo(out);
            }
        }
        if (getResult == null) {
            out.writeBoolean(false);
        } else {
            out.writeBoolean(true);
            getResult.writeTo(out);
        }
    }
}
