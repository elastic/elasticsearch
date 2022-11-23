/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.repositories;

import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.index.Index;
import org.elasticsearch.xcontent.ToXContent;
import org.elasticsearch.xcontent.ToXContentObject;
import org.elasticsearch.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.Objects;

/**
 * Represents a single snapshotted index in the repository.
 */
public final class IndexId implements Writeable, ToXContentObject {
    protected static final String NAME = "name";
    protected static final String ID = "id";

    private final String name;
    private final String id;
    private final int hashCode;

    public IndexId(final String name, final String id) {
        this.name = name;
        this.id = id;
        this.hashCode = computeHashCode();

    }

    public IndexId(final StreamInput in) throws IOException {
        this.name = in.readString();
        this.id = in.readString();
        this.hashCode = computeHashCode();
    }

    /**
     * The name of the index.
     */
    public String getName() {
        return name;
    }

    /**
     * The unique ID for the index within the repository.  This is *not* the same as the
     * index's UUID, but merely a unique file/URL friendly identifier that a repository can
     * use to name blobs for the index.
     *
     * We could not use the index's actual UUID (See {@link Index#getUUID()}) because in the
     * case of snapshot/restore, the index UUID in the snapshotted index will be different
     * from the index UUID assigned to it when it is restored. Hence, the actual index UUID
     * is not useful in the context of snapshot/restore for tying a snapshotted index to the
     * index it was snapshot from, and so we are using a separate UUID here.
     */
    public String getId() {
        return id;
    }

    @Override
    public String toString() {
        return "[" + name + "/" + id + "]";
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        IndexId that = (IndexId) o;
        return Objects.equals(name, that.name) && Objects.equals(id, that.id);
    }

    @Override
    public int hashCode() {
        return hashCode;
    }

    private int computeHashCode() {
        return Objects.hash(name, id);
    }

    @Override
    public void writeTo(final StreamOutput out) throws IOException {
        out.writeString(name);
        out.writeString(id);
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, ToXContent.Params params) throws IOException {
        builder.startObject();
        builder.field(NAME, name);
        builder.field(ID, id);
        builder.endObject();
        return builder;
    }
}
