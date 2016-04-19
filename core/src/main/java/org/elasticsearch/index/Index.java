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

package org.elasticsearch.index;

import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;

import java.io.IOException;

/**
 *
 */
public class Index implements Writeable<Index> {

    public static final Index[] EMPTY_ARRAY = new Index[0];

    private final String name;
    private final String uuid;

    public Index(String name, String uuid) {
        this.name = name.intern();
        this.uuid = uuid.intern();
    }

    /**
     * Read from a stream.
     */
    public Index(StreamInput in) throws IOException {
        this.name = in.readString();
        this.uuid = in.readString();
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeString(name);
        out.writeString(uuid);
    }

    public String getName() {
        return this.name;
    }

    public String getUUID() {
        return uuid;
    }

    @Override
    public String toString() {
        /*
         * If we have a uuid we put it in the toString so it'll show up in logs which is useful as more and more things use the uuid rather
         * than the name as the lookup key for the index.
         */
        if (ClusterState.UNKNOWN_UUID.equals(uuid)) {
            return "[" + name + "]";
        }
        return "[" + name + "/" + uuid + "]";
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null) {
            return false;
        }
        Index index1 = (Index) o;
        return uuid.equals(index1.uuid) && name.equals(index1.name); // allow for _na_ uuid
    }

    @Override
    public int hashCode() {
        int result = name.hashCode();
        result = 31 * result + uuid.hashCode();
        return result;
    }
}
