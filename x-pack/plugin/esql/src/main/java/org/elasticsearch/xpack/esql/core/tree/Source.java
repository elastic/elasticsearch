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
import org.elasticsearch.compute.operator.WarningSourceLocation;
import org.elasticsearch.xpack.esql.core.QlIllegalArgumentException;
import org.elasticsearch.xpack.esql.core.util.StringUtils;
import org.elasticsearch.xpack.esql.io.stream.PlanStreamInput;
import org.elasticsearch.xpack.esql.session.Configuration;

import java.io.IOException;
import java.util.Objects;

public final class Source implements Writeable, WarningSourceLocation {

    public static final Source EMPTY = new Source(Location.EMPTY, "", null);

    private final Location location;
    private final String text;
    /**
     * The name of the view this source came from, or null if it came from the original query.
     * This is used during serialization to look up the correct query string for reconstructing
     * the source text.
     */
    private final String viewName;

    public Source(int line, int charPositionInLine, String text) {
        this(new Location(line, charPositionInLine), text, null);
    }

    public Source(Location location, String text) {
        this(location, text, null);
    }

    public Source(Location location, String text, String viewName) {
        this.location = location;
        this.text = text;
        this.viewName = viewName;
    }

    /**
     * Returns a new Source with the same location and text but associated with the given view name.
     * This is used during view resolution to tag Source objects with their origin view.
     */
    public Source withViewName(String viewName) {
        if (this == EMPTY) {
            return this;
        }
        return new Source(location, text, viewName);
    }

    public static Source readFrom(PlanStreamInput in) throws IOException {
        if (in.readBoolean() == false) {
            return EMPTY;
        }
        SourcePositions positions = new SourcePositions(in);
        String viewName = null;
        if (in.getTransportVersion().supports(Configuration.ESQL_VIEW_QUERIES)) {
            viewName = in.readOptionalString();
        }
        int charPositionInLine = positions.column - 1;

        String query = in.sourceText(viewName);
        String text = sourceText(query, positions.line, positions.column, positions.length);
        return new Source(new Location(positions.line, charPositionInLine), text, viewName);
    }

    /**
     * Read the components of a {@link Source} and throw it away, returning
     * {@link Source#EMPTY}. Use this when you will never use the {@link Source}
     * and there is no chance of getting a {@link PlanStreamInput}.
     */
    public static Source readEmpty(StreamInput in) throws IOException {
        if (in.readBoolean()) {
            // Read it and throw it away because we're always returning empty.
            new SourcePositions(in);
            if (in.getTransportVersion().supports(Configuration.ESQL_VIEW_QUERIES)) {
                in.readOptionalString(); // viewName
            }
        }
        return EMPTY;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        if (this == EMPTY) {
            out.writeBoolean(false);
            return;
        }
        out.writeBoolean(true);
        new SourcePositions(location.getLineNumber(), location.getColumnNumber(), text.length()).writeTo(out);
        if (out.getTransportVersion().supports(Configuration.ESQL_VIEW_QUERIES)) {
            out.writeOptionalString(viewName);
        }
    }

    // TODO: rename to location()
    public Location source() {
        return location;
    }

    @Override
    public String text() {
        return text;
    }

    /**
     * Returns the name of the view this source came from, or null if it came from the original query.
     */
    @Override
    public String viewName() {
        return viewName;
    }

    @Override
    public int lineNumber() {
        return location.getLineNumber();
    }

    @Override
    public int columnNumber() {
        return location.getColumnNumber();
    }

    @Override
    public int hashCode() {
        return Objects.hash(location, text, viewName);
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
        return Objects.equals(location, other.location) && Objects.equals(text, other.text) && Objects.equals(viewName, other.viewName);
    }

    @Override
    public String toString() {
        return text + location;
    }

    /**
     * @deprecated Sources created by this can't be correctly deserialized. For use in tests only.
     */
    @Deprecated
    public static Source synthetic(String text) {
        return new Source(Location.EMPTY, text, null);
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

    /**
     * Offsets into the source string that we use for serialization.
     */
    private record SourcePositions(int line, int column, int length) implements Writeable {
        SourcePositions(StreamInput in) throws IOException {
            this(in.readInt(), in.readInt(), in.readInt());
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            out.writeInt(line);
            out.writeInt(column);
            out.writeInt(length);
        }
    }
}
