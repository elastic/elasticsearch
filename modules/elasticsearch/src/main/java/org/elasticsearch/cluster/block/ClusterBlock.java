/*
 * Licensed to Elastic Search and Shay Banon under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. Elastic Search licenses this
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

package org.elasticsearch.cluster.block;

import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Streamable;

import java.io.IOException;
import java.io.Serializable;

/**
 * @author kimchy (shay.banon)
 */
public class ClusterBlock implements Serializable, Streamable {

    private int id;

    private String description;

    private ClusterBlock() {
    }

    public ClusterBlock(int id, String description) {
        this.id = id;
        this.description = description;
    }

    public int id() {
        return this.id;
    }

    public String description() {
        return this.description;
    }

    public static ClusterBlock readClusterBlock(StreamInput in) throws IOException {
        ClusterBlock block = new ClusterBlock();
        block.readFrom(in);
        return block;
    }

    @Override public void readFrom(StreamInput in) throws IOException {
        id = in.readVInt();
        description = in.readUTF();
    }

    @Override public void writeTo(StreamOutput out) throws IOException {
        out.writeVInt(id);
        out.writeUTF(description);
    }

    @Override public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        ClusterBlock that = (ClusterBlock) o;

        if (id != that.id) return false;

        return true;
    }

    @Override public int hashCode() {
        return id;
    }
}
