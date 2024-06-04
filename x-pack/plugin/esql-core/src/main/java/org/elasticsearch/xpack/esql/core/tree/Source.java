/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.core.tree;

import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.xpack.esql.core.QlIllegalArgumentException;
import org.elasticsearch.xpack.esql.core.util.PlanStreamInput;
import org.elasticsearch.xpack.esql.core.util.StringUtils;

import java.io.IOException;
import java.util.Objects;

public final class Source implements Writeable {

    public static final Source EMPTY = new Source(Location.EMPTY, "");

    private final Location location;
    private final String text;

    public Source(int line, int charPositionInLine, String text) {
        this(new Location(line, charPositionInLine), text);
    }

    public Source(Location location, String text) {
        this.location = location;
        this.text = text;
    }

    public static <S extends StreamInput & PlanStreamInput> Source readFrom(S in) throws IOException {
        /*
         * The funny typing dance with `<S extends...>` is required we're in esql-core
         * here and the real PlanStreamInput is in esql-proper. And we need PlanStreamInput
         * to send the query one time.
         */
        if (in.readBoolean() == false) {
            return EMPTY;
        }
        int line = in.readInt();
        int column = in.readInt();
        int charPositionInLine = column - 1;

        int length = in.readInt();
        String text = sourceText(in.sourceText(), line, column, length);
        return new Source(new Location(line, charPositionInLine), text);
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        if (this == EMPTY) {
            out.writeBoolean(false);
            return;
        }
        out.writeBoolean(true);
        out.writeInt(location.getLineNumber());
        out.writeInt(location.getColumnNumber());
        out.writeInt(text.length());
    }

    // TODO: rename to location()
    public Location source() {
        return location;
    }

    public String text() {
        return text;
    }

    @Override
    public int hashCode() {
        return Objects.hash(location, text);
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }

        if (obj == null || getClass() != obj.getClass()) {
            return false;
        }

        Source other = (Source) obj;
        return Objects.equals(location, other.location) && Objects.equals(text, other.text);
    }

    @Override
    public String toString() {
        return text + location;
    }

    public static Source synthetic(String text) {
        return new Source(Location.EMPTY, text);
    }

    private static String sourceText(String query, int line, int column, int length) {
        if (line <= 0 || column <= 0 || query.isEmpty()) {
            return StringUtils.EMPTY;
        }
        int offset = textOffset(query, line, column);
        if (offset + length > query.length()) {
            throw new QlIllegalArgumentException(
                "location [@" + line + ":" + column + "] and length [" + length + "] overrun query size [" + query.length() + "]"
            );
        }
        return query.substring(offset, offset + length);
    }

    private static int textOffset(String query, int line, int column) {
        int offset = 0;
        if (line > 1) {
            String[] lines = query.split("\n");
            if (line > lines.length) {
                throw new QlIllegalArgumentException(
                    "line location [" + line + "] higher than max [" + lines.length + "] in query [" + query + "]"
                );
            }
            for (int i = 0; i < line - 1; i++) {
                offset += lines[i].length() + 1; // +1 accounts for the removed \n
            }
        }
        offset += column - 1; // -1 since column is 1-based indexed
        return offset;
    }

}
