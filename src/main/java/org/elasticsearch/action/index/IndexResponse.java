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

package org.elasticsearch.action.index;

import com.google.common.collect.ImmutableList;
import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * A response of an index operation,
 *
 * @see org.elasticsearch.action.index.IndexRequest
 * @see org.elasticsearch.client.Client#index(IndexRequest)
 */
public class IndexResponse extends ActionResponse {

    private String index;
    private String id;
    private String type;
    private long version;
    private List<String> matches;

    public IndexResponse() {

    }

    public IndexResponse(String index, String type, String id, long version) {
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
    public List<String> getMatches() {
        return this.matches;
    }

    /**
     * Internal.
     */
    public void setMatches(List<String> matches) {
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
                matches = ImmutableList.of(in.readString());
            } else if (size == 2) {
                matches = ImmutableList.of(in.readString(), in.readString());
            } else if (size == 3) {
                matches = ImmutableList.of(in.readString(), in.readString(), in.readString());
            } else if (size == 4) {
                matches = ImmutableList.of(in.readString(), in.readString(), in.readString(), in.readString());
            } else if (size == 5) {
                matches = ImmutableList.of(in.readString(), in.readString(), in.readString(), in.readString(), in.readString());
            } else {
                matches = new ArrayList<String>();
                for (int i = 0; i < size; i++) {
                    matches.add(in.readString());
                }
            }
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
            for (String match : matches) {
                out.writeString(match);
            }
        }
    }
}
