/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.qlcore.util;

import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.xpack.qlcore.QlIllegalArgumentException;
import org.elasticsearch.xpack.qlcore.tree.Location;
import org.elasticsearch.xpack.qlcore.tree.Source;

import java.io.IOException;

public final class SourceUtils {

    private SourceUtils() {}

    public static void writeSource(StreamOutput out, Source source) throws IOException {
        writeSource(out, source, true);
    }

    public static void writeSourceNoText(StreamOutput out, Source source) throws IOException {
        writeSource(out, source, false);
    }

    public static Source readSource(StreamInput in) throws IOException {
        return readSource(in, null);
    }

    public static Source readSourceWithText(StreamInput in, String queryText) throws IOException {
        return readSource(in, queryText);
    }

    private static void writeSource(StreamOutput out, Source source, boolean writeText) throws IOException {
        out.writeInt(source.source().getLineNumber());
        out.writeInt(source.source().getColumnNumber());
        if (writeText) {
            out.writeString(source.text());
        } else {
            out.writeInt(source.text().length());
        }
    }

    private static Source readSource(StreamInput in, @Nullable String queryText) throws IOException {
        int line = in.readInt();
        int column = in.readInt();
        int charPositionInLine = column - 1;

        String text;
        if (queryText == null) {
            text = in.readString();
        } else {
            int length = in.readInt();
            text = sourceText(queryText, line, column, length);
        }
        return new Source(new Location(line, charPositionInLine), text);
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
