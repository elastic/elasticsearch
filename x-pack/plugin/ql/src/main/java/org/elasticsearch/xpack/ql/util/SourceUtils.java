/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.ql.util;

import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.xpack.ql.QlIllegalArgumentException;
import org.elasticsearch.xpack.ql.tree.Location;
import org.elasticsearch.xpack.ql.tree.Source;

import java.io.IOException;

public final class SourceUtils {

    private SourceUtils() {}

    public static void writeSource(StreamOutput out, Source source) throws IOException {
        out.writeInt(source.source().getLineNumber());
        out.writeInt(source.source().getColumnNumber());
        out.writeString(source.text());
    }

    public static Source readSource(StreamInput in) throws IOException {
        int line = in.readInt();
        int column = in.readInt();
        int charPositionInLine = column - 1;
        String text = in.readString();
        return new Source(new Location(line, charPositionInLine), text);
    }

    public static void writeSourceNoText(StreamOutput out, Source source) throws IOException {
        out.writeInt(source.source().getLineNumber());
        out.writeInt(source.source().getColumnNumber());
        out.writeInt(source.text().length());
    }

    public static Source readSourceWithText(StreamInput in, String queryText) throws IOException {
        int line = in.readInt();
        int column = in.readInt();
        int length = in.readInt();
        int charPositionInLine = column - 1;
        return new Source(new Location(line, charPositionInLine), sourceText(queryText, line, column, length));
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
