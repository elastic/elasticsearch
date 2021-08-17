/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.cluster.metadata;

import org.elasticsearch.common.Strings;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.xcontent.ToXContentObject;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.core.Nullable;

import java.io.IOException;
import java.util.Collection;
import java.util.HashSet;
import java.util.Objects;
import java.util.Set;

/**
 * A class encapsulating the usage of a particular "thing" by something else
 */
public class ItemUsage implements Writeable, ToXContentObject {

    private final Set<String> indices;
    private final Set<String> dataStreams;
    private final Set<String> composableTemplates;

    /**
     * Create a new usage, a {@code null} value indicates that the item *cannot* be used by the
     * thing, otherwise use an empty collection to indicate no usage.
     */
    public ItemUsage(@Nullable Collection<String> indices,
                     @Nullable Collection<String> dataStreams,
                     @Nullable Collection<String> composableTemplates) {
        this.indices = indices == null ? null : new HashSet<>(indices);
        this.dataStreams = dataStreams == null ? null : new HashSet<>(dataStreams);
        this.composableTemplates = composableTemplates == null ? null : new HashSet<>(composableTemplates);
    }

    public ItemUsage(StreamInput in) throws IOException {
        if (in.readBoolean()) {
            this.indices = in.readSet(StreamInput::readString);
        } else {
            this.indices = null;
        }
        if (in.readBoolean()) {
            this.dataStreams = in.readSet(StreamInput::readString);
        } else {
            this.dataStreams = null;
        }
        if (in.readBoolean()) {
            this.composableTemplates = in.readSet(StreamInput::readString);
        } else {
            this.composableTemplates = null;
        }
    }

    public Set<String> getIndices() {
        return indices;
    }

    public Set<String> getDataStreams() {
        return dataStreams;
    }

    public Set<String> getComposableTemplates() {
        return composableTemplates;
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        if (this.indices != null) {
            builder.field("indices", this.indices);
        }
        if (this.dataStreams != null) {
            builder.field("data_streams", this.dataStreams);
        }
        if (this.composableTemplates != null) {
            builder.field("composable_templates", this.composableTemplates);
        }
        builder.endObject();
        return builder;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeOptionalStringCollection(this.indices);
        out.writeOptionalStringCollection(this.dataStreams);
        out.writeOptionalStringCollection(this.composableTemplates);
    }

    @Override
    public int hashCode() {
        return Objects.hash(this.indices, this.dataStreams, this.composableTemplates);
    }

    @Override
    public boolean equals(Object obj) {
        if (obj == null) {
            return false;
        }
        if (getClass() != obj.getClass()) {
            return false;
        }
        ItemUsage other = (ItemUsage) obj;
        return Objects.equals(indices, other.indices) &&
            Objects.equals(dataStreams, other.dataStreams) &&
            Objects.equals(composableTemplates, other.composableTemplates);
    }

    @Override
    public String toString() {
        return Strings.toString(this);
    }
}
