/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.search.fetch.subphase.highlight;

import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.xcontent.Text;
import org.elasticsearch.xcontent.ToXContentFragment;
import org.elasticsearch.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.Arrays;
import java.util.Objects;

/**
 * A field highlighted with its highlighted fragments.
 */
public class HighlightField implements ToXContentFragment, Writeable {

    private final String name;

    private final Text[] fragments;

    public HighlightField(StreamInput in) throws IOException {
        this(in.readString(), in.readOptionalArray(StreamInput::readText, Text[]::new));
    }

    public HighlightField(String name, Text[] fragments) {
        this.name = Objects.requireNonNull(name, "missing highlight field name");
        this.fragments = fragments;
    }

    /**
     * The name of the field highlighted.
     */
    public String name() {
        return name;
    }

    /**
     * The highlighted fragments. {@code null} if failed to highlight (for example, the field is not stored).
     */
    public Text[] fragments() {
        return fragments;
    }

    @Override
    public String toString() {
        return "[" + name + "], fragments[" + Arrays.toString(fragments) + "]";
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeString(name);
        if (fragments == null) {
            out.writeBoolean(false);
        } else {
            out.writeBoolean(true);
            out.writeArray(StreamOutput::writeText, fragments);
        }
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.field(name);
        if (fragments == null) {
            builder.nullValue();
        } else {
            builder.startArray();
            for (Text fragment : fragments) {
                builder.value(fragment);
            }
            builder.endArray();
        }
        return builder;
    }

    @Override
    public final boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (obj == null || getClass() != obj.getClass()) {
            return false;
        }
        HighlightField other = (HighlightField) obj;
        return Objects.equals(name, other.name) && Arrays.equals(fragments, other.fragments);
    }

    @Override
    public final int hashCode() {
        return Objects.hash(name, Arrays.hashCode(fragments));
    }

}
