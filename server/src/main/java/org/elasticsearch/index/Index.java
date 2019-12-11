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
import org.elasticsearch.common.ParseField;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.xcontent.ObjectParser;
import org.elasticsearch.common.xcontent.ToXContentObject;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentParser;

import java.io.IOException;
import java.util.Objects;

/**
 * A value class representing the basic required properties of an Elasticsearch index.
 */
public class Index implements Writeable, ToXContentObject {

    public static final Index[] EMPTY_ARRAY = new Index[0];
    private static final String INDEX_UUID_KEY = "index_uuid";
    private static final String INDEX_NAME_KEY = "index_name";
    private static final ObjectParser<Builder, Void> INDEX_PARSER = new ObjectParser<>("index", Builder::new);
    static {
        INDEX_PARSER.declareString(Builder::name, new ParseField(INDEX_NAME_KEY));
        INDEX_PARSER.declareString(Builder::uuid, new ParseField(INDEX_UUID_KEY));
    }

    private final String name;
    private final String uuid;

    public Index(String name, String uuid) {
        this.name = Objects.requireNonNull(name);
        this.uuid = Objects.requireNonNull(uuid);
    }

    /**
     * Read from a stream.
     */
    public Index(StreamInput in) throws IOException {
        this.name = in.readString();
        this.uuid = in.readString();
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
        if (o == null || getClass() != o.getClass()) {
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

    @Override
    public void writeTo(final StreamOutput out) throws IOException {
        out.writeString(name);
        out.writeString(uuid);
    }

    @Override
    public XContentBuilder toXContent(final XContentBuilder builder, final Params params) throws IOException {
        builder.startObject();
        builder.field(INDEX_NAME_KEY, name);
        builder.field(INDEX_UUID_KEY, uuid);
        return builder.endObject();
    }

    public static Index fromXContent(final XContentParser parser) throws IOException {
        return INDEX_PARSER.parse(parser, null).build();
    }

    /**
     * Builder for Index objects.  Used by ObjectParser instances only.
     */
    private static final class Builder {
        private String name;
        private String uuid;

        public void name(final String name) {
            this.name = name;
        }

        public void uuid(final String uuid) {
            this.uuid = uuid;
        }

        public Index build() {
            return new Index(name, uuid);
        }
    }
}
