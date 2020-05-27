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

package org.elasticsearch.search.internal;

import org.elasticsearch.Version;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;

import java.io.IOException;
import java.util.Objects;


public final class SearchContextId implements Writeable {
    private final String readerId;
    private final long id;

    public SearchContextId(String readerId, long id) {
        this.readerId = Objects.requireNonNull(readerId);
        this.id = id;
    }

    public SearchContextId(StreamInput in) throws IOException {
        this.id = in.readLong();
        if (in.getVersion().onOrAfter(Version.V_7_7_0)) {
            this.readerId = in.readString();
        } else {
            this.readerId = "";
        }
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeLong(id);
        if (out.getVersion().onOrAfter(Version.V_7_7_0)) {
            out.writeString(readerId);
        }
    }

    public String getReaderId() {
        return readerId;
    }

    public long getId() {
        return id;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        SearchContextId other = (SearchContextId) o;
        return id == other.id && readerId.equals(other.readerId);
    }

    @Override
    public int hashCode() {
        return Objects.hash(readerId, id);
    }

    @Override
    public String toString() {
        return "[" + readerId + "][" + id + "]";
    }
}
