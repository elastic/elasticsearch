/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.action.suggestions;

import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.xcontent.XContentParserUtils;
import org.elasticsearch.xcontent.XContentParseException;
import org.elasticsearch.xcontent.XContentParser;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;

/**
 * A cursor position expressed in a caller-chosen offset unit, so clients that natively index strings
 * differently (Python: code points; Go/Rust/C: UTF-8 bytes; JS/Java/C#/Kotlin: UTF-16 code units)
 * never have to convert into a foreign unit just to call the suggestions endpoint.
 *
 * <p>Wire shape is a nested, tagged object with exactly one key: {@code {"cursor": {"utf16": 12}}},
 * {@code {"cursor": {"utf8": 14}}}, or {@code {"cursor": {"codepoint": 11}}}. There is no default unit
 * and no fallback: zero keys, more than one key, or an unrecognized key are all rejected as a parse
 * error, not a validation error, since the problem is with the request's shape rather than a value.
 *
 * <p>{@link #resolve(String)} converts to the plain UTF-16 offset every downstream consumer
 * ({@link CursorLocation}, {@code SuggestionContext}, {@code SuggestionBuilder},
 * {@code HotTierValueSampler}) works in, exactly once, at the request boundary. None of that downstream
 * machinery is unit-aware; only this class is.
 */
public record CursorOffset(Unit unit, int offset) {

    public enum Unit {
        UTF8,
        UTF16,
        CODE_POINT
    }

    public static CursorOffset utf16(int offset) {
        return new CursorOffset(Unit.UTF16, offset);
    }

    public static CursorOffset utf8(int offset) {
        return new CursorOffset(Unit.UTF8, offset);
    }

    public static CursorOffset codePoint(int offset) {
        return new CursorOffset(Unit.CODE_POINT, offset);
    }

    public static CursorOffset readFrom(StreamInput in) throws IOException {
        return new CursorOffset(in.readEnum(Unit.class), in.readVInt());
    }

    public void writeTo(StreamOutput out) throws IOException {
        out.writeEnum(unit);
        out.writeVInt(offset);
    }

    /**
     * Resolve this offset to a plain UTF-16 offset into {@code query}, the unit every downstream
     * consumer works in. Throws {@link IllegalArgumentException} (naming the unit) if the offset is out
     * of range for its unit, or — for {@link Unit#UTF8} — splits a multi-byte UTF-8 sequence rather than
     * landing on a code-point boundary.
     */
    public int resolve(String query) {
        return switch (unit) {
            case UTF16 -> resolveUtf16(query);
            case CODE_POINT -> resolveCodePoint(query);
            case UTF8 -> resolveUtf8(query);
        };
    }

    private int resolveUtf16(String query) {
        if (offset < 0 || offset > query.length()) {
            throw new IllegalArgumentException("[cursor.utf16] must be within [0, " + query.length() + "], got [" + offset + "]");
        }
        return offset;
    }

    private int resolveCodePoint(String query) {
        int codePointCount = query.codePointCount(0, query.length());
        if (offset < 0 || offset > codePointCount) {
            throw new IllegalArgumentException("[cursor.codepoint] must be within [0, " + codePointCount + "], got [" + offset + "]");
        }
        return query.offsetByCodePoints(0, offset);
    }

    private int resolveUtf8(String query) {
        byte[] bytes = query.getBytes(StandardCharsets.UTF_8);
        if (offset < 0 || offset > bytes.length) {
            throw new IllegalArgumentException("[cursor.utf8] must be within [0, " + bytes.length + "], got [" + offset + "]");
        }
        int byteCount = 0;
        int charOffset = 0;
        while (charOffset < query.length()) {
            if (byteCount == offset) {
                return charOffset;
            }
            int codePoint = query.codePointAt(charOffset);
            int byteLength = utf8Length(codePoint);
            if (byteCount + byteLength > offset) {
                throw new IllegalArgumentException(
                    "[cursor.utf8] offset [" + offset + "] splits a multi-byte UTF-8 sequence starting at byte [" + byteCount + "]"
                );
            }
            byteCount += byteLength;
            charOffset += Character.charCount(codePoint);
        }
        // byteCount == bytes.length == offset here, or offset was rejected above.
        return charOffset;
    }

    private static int utf8Length(int codePoint) {
        if (codePoint <= 0x7F) {
            return 1;
        }
        if (codePoint <= 0x7FF) {
            return 2;
        }
        if (codePoint <= 0xFFFF) {
            return 3;
        }
        return 4;
    }

    /**
     * Parse the tagged {@code cursor} object. {@code parser} must be positioned at the object's
     * {@code START_OBJECT} token (the convention {@link org.elasticsearch.xcontent.ObjectParser
     * #declareObject} hands a nested-object field's entry parser).
     */
    public static CursorOffset fromXContent(XContentParser parser) throws IOException {
        XContentParserUtils.ensureExpectedToken(XContentParser.Token.START_OBJECT, parser.currentToken(), parser);
        List<String> seenKeys = new ArrayList<>();
        Unit unit = null;
        int offset = 0;
        for (XContentParser.Token token = parser.nextToken(); token != XContentParser.Token.END_OBJECT; token = parser.nextToken()) {
            if (token != XContentParser.Token.FIELD_NAME) {
                continue;
            }
            String name = parser.currentName();
            Unit candidate = unitForKey(name);
            if (candidate == null) {
                throw new XContentParseException(
                    parser.getTokenLocation(),
                    "unknown cursor unit [" + name + "], expected one of utf8/utf16/codepoint"
                );
            }
            parser.nextToken();
            offset = parser.intValue();
            unit = candidate;
            seenKeys.add(name);
        }
        if (seenKeys.isEmpty()) {
            throw new XContentParseException(
                parser.getTokenLocation(),
                "cursor must specify exactly one of utf8/utf16/codepoint, got no unit"
            );
        }
        if (seenKeys.size() > 1) {
            throw new XContentParseException(
                parser.getTokenLocation(),
                "cursor must specify exactly one of utf8/utf16/codepoint, got: " + seenKeys
            );
        }
        return new CursorOffset(unit, offset);
    }

    private static Unit unitForKey(String key) {
        return switch (key.toLowerCase(Locale.ROOT)) {
            case "utf8" -> Unit.UTF8;
            case "utf16" -> Unit.UTF16;
            case "codepoint" -> Unit.CODE_POINT;
            default -> null;
        };
    }

    /** The wire key for this offset's unit, e.g. for round-tripping back to JSON. */
    public String wireKey() {
        return switch (unit) {
            case UTF8 -> "utf8";
            case UTF16 -> "utf16";
            case CODE_POINT -> "codepoint";
        };
    }
}
