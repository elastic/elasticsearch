/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.core.util;

import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.xpack.esql.core.tree.Location;
import org.elasticsearch.xpack.esql.core.tree.Source;

import java.io.IOException;

public final class SourceUtils {

    private SourceUtils() {}

    /**
     * Write a {@link Source} including the text in it.
     * @deprecated replace with {@link Source#writeTo}.
     *             That's not binary compatible so the replacement is complex.
     */
    @Deprecated
    public static void writeSource(StreamOutput out, Source source) throws IOException {
        out.writeInt(source.source().getLineNumber());
        out.writeInt(source.source().getColumnNumber());
        out.writeString(source.text());
    }

    /**
     * Read a {@link Source} including the text in it.
     * @deprecated replace with {@link Source#readFrom(StreamInput)}.
     *             That's not binary compatible so the replacement is complex.
     */
    @Deprecated
    public static Source readSource(StreamInput in) throws IOException {
        int line = in.readInt();
        int column = in.readInt();
        int charPositionInLine = column - 1;

        String text = in.readString();
        return new Source(new Location(line, charPositionInLine), text);
    }
}
