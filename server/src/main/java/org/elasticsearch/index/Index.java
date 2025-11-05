/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index;

import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.xcontent.ObjectParser;
import org.elasticsearch.xcontent.ParseField;
import org.elasticsearch.xcontent.ToXContentObject;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentParser;

import java.io.IOException;
import java.util.Comparator;
import java.util.Objects;

/**
 * A value class representing the basic required properties of an Elasticsearch index.
 */
public class Index implements Writeable, ToXContentObject {

    public static final Index[] EMPTY_ARRAY = new Index[0];

    public static final Comparator<Index> COMPARE_BY_NAME = Comparator.comparing(Index::getName);

    private static final String INDEX_UUID_KEY = "index_uuid";
    private static final String INDEX_NAME_KEY = "index_name";
    private static final ObjectParser<Builder, Void> INDEX_PARSER = new ObjectParser<>("index", Builder::new);
    static {
        INDEX_PARSER.declareString(Builder::name, new ParseField(INDEX_NAME_KEY));
        INDEX_PARSER.declareString(Builder::uuid, new ParseField(INDEX_UUID_KEY));
    }

    private final String name;
    private final String uuid;

    /**
     * Constructs a new Index with the specified name and UUID.
     *
     * @param name the name of the index, must not be null
     * @param uuid the unique identifier of the index, must not be null
     * @throws NullPointerException if name or uuid is null
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * Index index = new Index("my-index", "abc123-def456");
     * }</pre>
     */
    public Index(String name, String uuid) {
        this.name = Objects.requireNonNull(name);
        this.uuid = Objects.requireNonNull(uuid);
    }

    /**
     * Constructs an Index by reading from a stream.
     * Deserializes the index name and UUID from the provided input stream.
     *
     * @param in the stream to read from
     * @throws IOException if an I/O error occurs while reading from the stream
     */
    public Index(StreamInput in) throws IOException {
        this.name = in.readString();
        this.uuid = in.readString();
    }

    /**
     * Retrieves the name of this index.
     *
     * @return the index name
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * Index index = new Index("my-index", "abc123");
     * String name = index.getName(); // Returns "my-index"
     * }</pre>
     */
    public String getName() {
        return this.name;
    }

    /**
     * Retrieves the unique identifier (UUID) of this index.
     *
     * @return the index UUID
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * Index index = new Index("my-index", "abc123");
     * String uuid = index.getUUID(); // Returns "abc123"
     * }</pre>
     */
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

    /**
     * Serializes this index to the provided output stream.
     * Writes the index name and UUID in order.
     *
     * @param out the output stream to write to
     * @throws IOException if an I/O error occurs during serialization
     */
    @Override
    public void writeTo(final StreamOutput out) throws IOException {
        out.writeString(name);
        out.writeString(uuid);
    }

    /**
     * Converts this index to XContent format as a complete object.
     * The output includes both the index name and UUID.
     *
     * @param builder the XContent builder to write to
     * @param params additional parameters for the conversion (unused)
     * @return the XContent builder for method chaining
     * @throws IOException if an I/O error occurs during conversion
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * Index index = new Index("my-index", "abc123");
     * XContentBuilder builder = XContentFactory.jsonBuilder();
     * index.toXContent(builder, ToXContent.EMPTY_PARAMS);
     * // Result: {"index_name":"my-index","index_uuid":"abc123"}
     * }</pre>
     */
    @Override
    public XContentBuilder toXContent(final XContentBuilder builder, final Params params) throws IOException {
        builder.startObject();
        toXContentFragment(builder);
        return builder.endObject();
    }

    /**
     * Converts this index to XContent format as a fragment (without wrapping object).
     * Useful when embedding index information within a larger XContent structure.
     *
     * @param builder the XContent builder to write to
     * @return the XContent builder for method chaining
     * @throws IOException if an I/O error occurs during conversion
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * Index index = new Index("my-index", "abc123");
     * XContentBuilder builder = XContentFactory.jsonBuilder().startObject();
     * index.toXContentFragment(builder);
     * builder.endObject();
     * }</pre>
     */
    public XContentBuilder toXContentFragment(final XContentBuilder builder) throws IOException {
        builder.field(INDEX_NAME_KEY, name);
        builder.field(INDEX_UUID_KEY, uuid);
        return builder;
    }

    /**
     * Parses an Index from XContent format.
     * Expects the XContent to contain both "index_name" and "index_uuid" fields.
     *
     * @param parser the XContent parser to read from
     * @return the parsed Index object
     * @throws IOException if an I/O error occurs or the content is malformed
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * XContentParser parser = ... // parser with content {"index_name":"my-index","index_uuid":"abc123"}
     * Index index = Index.fromXContent(parser);
     * }</pre>
     */
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
