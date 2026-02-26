/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.expression.function.scalar.string;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * Parsed path for {@link JsonExtract}. Takes a path string like {@code "user.address[0].city"}
 * and breaks it into a list of typed {@link Segment}s that {@link JsonExtract} walks to
 * navigate a JSON document.
 * <p>
 * This class implements a subset of
 * <a href="https://datatracker.ietf.org/doc/rfc9535/">JSONPath (RFC 9535)</a>.
 * The supported syntax is:
 * <ul>
 *   <li><b>Dot notation</b> for named fields: {@code user.address.city}</li>
 *   <li><b>Bracket notation</b> for zero-based array indices: {@code items[0]}, {@code items[42]}</li>
 *   <li><b>Quoted bracket notation</b> for keys containing special characters (dots, spaces,
 *       brackets, etc.). Both single and double quotes are supported:
 *       {@code ['user.name']} and {@code ["user.name"]} are equivalent. Per RFC 9535, dot notation
 *       and quoted bracket notation are interchangeable for simple keys — {@code a.b} and
 *       {@code a['b']} and {@code a["b"]} all produce the same result. The quoted form is only
 *       required when the key contains characters that dot notation cannot express.
 *       Empty string keys are also supported ({@code ['']}) — per RFC 9535, an empty string
 *       is a valid member name.</li>
 *   <li><b>Escape sequences</b> inside quoted brackets: {@code \'} and {@code \"} produce a literal
 *       quote, {@code \\} produces a literal backslash. For example, {@code ['it\\'s']} accesses
 *       the key {@code it's}, and {@code ['a\\\\b']} accesses the key {@code a\b}.</li>
 *   <li><b>The optional {@code $} root selector</b>: {@code $.name} and {@code name} are equivalent.
 *       The three recognized prefix forms are {@code $.} (dot after dollar), {@code $[} (bracket
 *       after dollar), and bare {@code $} (the entire document). {@code $} alone, {@code $.} alone,
 *       and an empty string all mean "return the root value".</li>
 *   <li><b>Mixed notation</b>: any combination of the above in a single path, e.g.,
 *       {@code $.store['items'][0].name}.</li>
 * </ul>
 * <p>
 * Each segment is either a {@link Segment.Key} (navigate into an object by field name)
 * or a {@link Segment.Index} (navigate into an array by zero-based index). An empty
 * segment list means "return the root value" (paths like {@code ""}, {@code "$"}, or {@code "$."}).
 * <p>
 * Parsing happens in the constructor. When the path is a constant expression in a query,
 * this is constructed once and reused across all rows.
 *
 * <h2>Error reporting</h2>
 * Parse errors include the full path and a human-readable reason. An optional
 * {@code errorPositionOffset} shifts the reported character positions so they make sense
 * in the caller's coordinate system. For example, when the path is a constant in a query,
 * the caller can pass the column where the path literal starts, so the positions in error
 * messages refer to the query text rather than the path string.
 */
final class JsonPath {

    // Characters used by the parser, extracted as constants for readability.
    private static final char DOLLAR = '$';
    private static final char DOT = '.';
    private static final char OPEN_BRACKET = '[';
    private static final char CLOSE_BRACKET = ']';
    private static final char BACKSLASH = '\\';
    private static final char SINGLE_QUOTE = '\'';
    private static final char DOUBLE_QUOTE = '"';

    /**
     * A single step in a path. The sealed interface ensures exhaustive matching:
     * every segment is either a key (object field) or an index (array element).
     */
    sealed interface Segment {
        /** Navigate into a JSON object by field name. */
        record Key(String name) implements Segment {}

        /** Navigate into a JSON array by zero-based index. */
        record Index(int index) implements Segment {}
    }

    private final String originalPath;
    private final List<Segment> segments;

    private JsonPath(String originalPath, List<Segment> segments) {
        this.originalPath = originalPath;
        this.segments = segments;
    }

    /**
     * Parses the given path string into typed segments.
     * Error positions are relative to the path string itself (offset 0).
     *
     * @param path the path string to parse (e.g., {@code "user.name"}, {@code "$.items[0]"})
     * @throws IllegalArgumentException if the path is malformed
     */
    static JsonPath parse(String path) {
        return parse(path, 0);
    }

    /**
     * Parses the given path string into typed segments.
     *
     * @param path                the path string to parse
     * @param errorPositionOffset offset added to character positions in error messages, for reporting only.
     *                            Pass 0 when the path comes from data (dynamic); pass the column of the path
     *                            literal in the query when the path is a constant expression, so that error
     *                            positions are meaningful relative to the query text.
     * @throws IllegalArgumentException if the path is malformed
     */
    static JsonPath parse(String path, int errorPositionOffset) {
        return new JsonPath(path, Collections.unmodifiableList(doParse(path, errorPositionOffset)));
    }

    /** Returns the original path string as provided to the constructor. */
    String originalPath() {
        return originalPath;
    }

    /** Returns the parsed segments. An empty list means "return the root value". */
    List<Segment> segments() {
        return segments;
    }

    @Override
    public String toString() {
        return originalPath;
    }

    // ========================================
    // Parser
    // ========================================
    //
    // The parser converts a path string like "$.store['items'][0].name" into a list of
    // typed Segment values: [Key("store"), Key("items"), Index(0), Key("name")].
    //
    // It works in two phases:
    // 1. normalizePath() strips the optional "$" prefix. Recognizes three forms:
    // "$." → strip both characters ("$.name" → "name")
    // "$[" → strip only the dollar ("$['key']" → "['key']")
    // "$" → root accessor, returns null
    //
    // 2. parse() walks the remaining string character by character, dispatching to
    // specialized readers for each kind of segment:
    // - readKey() bare dot-notation keys: "name" in "user.name"
    // - readBracket() dispatches bracket content to either:
    // - readQuotedKey() single- or double-quoted keys: ['key'] or ["key"]
    // supports escape sequences: \' \" \\ inside quotes
    // - readIndex() non-negative integer array indices: [0], [42]
    // Per RFC 9535, optional whitespace is allowed inside brackets: [ 0 ], [ 'key' ]
    //
    // Note: dot notation and quoted bracket notation are interchangeable for simple keys,
    // per RFC 9535. "a.b", "a['b']", and "a[\"b\"]" all produce [Key("a"), Key("b")].
    // The quoted form exists for keys with special characters (dots, spaces, brackets).
    //
    // All positions reported in error messages are shifted by a base offset that accounts
    // for both the "$" prefix stripped by normalizePath() and the caller-provided
    // errorPositionOffset. This way, positions are always meaningful in the caller's
    // coordinate system (the query text for constants, the path string for dynamic values).

    /**
     * Strips the optional {@code $} prefix so the main parser doesn't need to deal with it.
     * Handles three prefix forms:
     * <ul>
     *   <li>{@code $.} — strip both characters: {@code "$.name"} → {@code "name"}</li>
     *   <li>{@code $[} — strip only the dollar, keep the bracket: {@code "$['key']"} → {@code "['key']"}</li>
     *   <li>bare {@code $} — root accessor, returns {@code null}</li>
     * </ul>
     * Also returns {@code null} for an empty string or {@code "$."} (after stripping, the
     * remainder is empty). A {@code null} return means "return the whole JSON document".
     * <p>
     * Rejects {@code $} followed by anything other than {@code .} or {@code [}, since
     * {@code $name} is not valid JSONPath per RFC 9535.
     *
     * @param path                the path string to normalize (also used in error messages)
     * @param errorPositionOffset caller-provided offset for error reporting only
     */
    private static String normalizePath(String path, int errorPositionOffset) {
        // Empty string or bare "$" → root accessor
        if (path.isEmpty() || path.charAt(0) == DOLLAR && path.length() == 1) {
            return null;
        }
        // Strip "$." or "$[" prefix — the dot or bracket is consumed as part of the prefix.
        // For "$[", we keep the "[" because the main parser needs it to enter bracket mode.
        if (path.length() >= 2 && path.charAt(0) == DOLLAR) {
            char second = path.charAt(1);
            if (second == DOT) {
                path = path.substring(2);    // "$.name" → "name"
            } else if (second == OPEN_BRACKET) {
                path = path.substring(1);    // "$['key']" → "['key']"
            } else {
                // "$name" is not valid JSONPath per RFC 9535 — $ must be followed by "." or "["
                throw invalidPath(path, "expected [.] or [[] after [$]", errorPositionOffset + 1);
            }
        }
        // After stripping "$.", the remainder could be empty (input was "$." exactly)
        return path.isEmpty() ? null : path;
    }

    /**
     * Main parser loop. Walks the normalized path character by character and builds the segment list.
     * <p>
     * Each iteration reads one segment and advances {@code pos} past it. The three cases are:
     * <ol>
     *   <li>A {@code .} — separator between segments in dot notation ({@code a.b})</li>
     *   <li>A {@code [} — start of bracket notation ({@code [0]}, {@code ['key']})</li>
     *   <li>Anything else — a bare key at the start of the path ({@code name...})</li>
     * </ol>
     *
     * @param originalPath        the path as the user wrote it (for error messages)
     * @param errorPositionOffset caller-provided offset for error reporting only
     */
    private static List<Segment> doParse(String originalPath, int errorPositionOffset) {
        String path = normalizePath(originalPath, errorPositionOffset);
        if (path == null) {
            return List.of(); // root accessor — no segments to walk
        }

        // Error positions need to account for characters stripped by normalizePath() (the "$." or "$["
        // prefix) plus the caller's external offset. We compute this once and thread it through.
        int baseOffset = errorPositionOffset + (originalPath.length() - path.length());

        List<Segment> segments = new ArrayList<>();
        int pos = 0;

        // Tracks whether we've already read at least one segment. After the first segment, the
        // next character must be a dot or bracket — a bare key without a preceding separator is invalid.
        boolean hasReadSegment = false;

        while (pos < path.length()) {
            char c = path.charAt(pos);
            if (c == DOT) {
                // Dot separator between segments: "a.b" — skip the dot, then read the next key.
                if (hasReadSegment == false) {
                    throw invalidPath(originalPath, "path cannot start with a dot", baseOffset + pos);
                }
                pos++; // skip the dot itself
                if (pos >= path.length()) {
                    throw invalidPath(originalPath, "path cannot end with a dot", baseOffset + pos - 1);
                }
                if (path.charAt(pos) == DOT) {
                    throw invalidPath(originalPath, "consecutive dots", baseOffset + pos - 1);
                }
                pos = readKey(path, originalPath, pos, baseOffset, segments);
                hasReadSegment = true;
            } else if (c == OPEN_BRACKET) {
                // Bracket notation: "[0]", "['key']" — can follow a segment directly without a dot separator.
                pos = readBracket(path, originalPath, pos + 1, baseOffset + pos, segments);
                hasReadSegment = true;
            } else {
                // Bare key at the very start of the path (before any dot or bracket).
                // This is only valid for the first segment — "name..." but not "a name..." (missing separator).
                if (hasReadSegment) {
                    throw invalidPath(originalPath, "expected [.] or [[] before [" + c + "]", baseOffset + pos);
                }
                pos = readKey(path, originalPath, pos, baseOffset, segments);
                hasReadSegment = true;
            }
        }

        if (segments.isEmpty()) {
            throw invalidPath(originalPath, "empty path");
        }
        return segments;
    }

    /**
     * Reads a bare (unquoted) key in dot notation. Scans forward from {@code start} until it hits
     * a separator ({@code .} or {@code [}), a stray {@code ]}, or end of string. The characters
     * between {@code start} and that boundary become a {@link Segment.Key}.
     * <p>
     * Bare keys can contain any characters except {@code .}, {@code [}, and {@code ]}. For simple
     * identifiers, dot notation and quoted bracket notation are interchangeable per RFC 9535 —
     * {@code a.b} and {@code a['b']} produce the same result. Keys with special characters (dots,
     * spaces, brackets) must use quoted bracket notation: {@code ['user.name']} rather than
     * {@code user.name} (which would be parsed as two separate keys).
     * <p>
     * For example, in {@code "user.name[0]"}, calling this at position 5 reads {@code "name"}
     * and returns position 9 (the {@code [}).
     *
     * @return the position immediately after the key (the next separator or end of string)
     */
    private static int readKey(String path, String originalPath, int start, int baseOffset, List<Segment> segments) {
        int end = start;
        while (end < path.length()) {
            char c = path.charAt(end);
            if (c == DOT || c == OPEN_BRACKET || c == CLOSE_BRACKET) {
                break;
            }
            end++;
        }
        if (end == start) {
            throw invalidPath(originalPath, "empty key name", baseOffset + start);
        }
        segments.add(new Segment.Key(path.substring(start, end)));
        return end;
    }

    /**
     * Dispatches bracket content to the appropriate reader. Called with {@code pos} pointing
     * to the first character after the opening {@code [}. Per RFC 9535, optional whitespace
     * is allowed after {@code [} and before {@code ]} (e.g., {@code [ 0 ]} and {@code [ 'key' ]}).
     * Leading whitespace is skipped here; trailing whitespace is handled by the individual readers.
     * Determines what kind of bracket expression this is by looking at the first non-whitespace character:
     * <ul>
     *   <li>Single quote ({@code '}) or double quote ({@code "}) → quoted key,
     *       delegates to {@link #readQuotedKey}. Example: {@code ['user.name']} or {@code ["user.name"]}</li>
     *   <li>Digit or other non-quote character → numeric array index,
     *       delegates to {@link #readIndex}. Example: {@code [0]}, {@code [42]}</li>
     *   <li>Immediate {@code ]} (empty brackets) → error</li>
     * </ul>
     *
     * @param bracketOffset the position of the opening {@code [} in the caller's coordinate system,
     *                      used for error reporting only — all bracket-related errors point back to
     *                      the {@code [} that started the expression
     * @return the position immediately after the closing {@code ]}
     */
    private static int readBracket(String path, String originalPath, int pos, int bracketOffset, List<Segment> segments) {
        // Per RFC 9535, optional whitespace is allowed after "[" and before "]":
        // bracketed-selection = "[" S selector *(S "," S selector) S "]"
        pos = skipBlankSpace(path, pos);
        if (pos >= path.length()) {
            throw invalidPath(originalPath, "unterminated bracket", bracketOffset);
        }
        char c = path.charAt(pos);
        if (c == SINGLE_QUOTE || c == DOUBLE_QUOTE) {
            return readQuotedKey(path, originalPath, pos, bracketOffset, segments);
        } else if (c == CLOSE_BRACKET) {
            throw invalidPath(originalPath, "empty brackets", bracketOffset);
        } else {
            return readIndex(path, originalPath, pos, bracketOffset, segments);
        }
    }

    /**
     * Reads a quoted key inside brackets. Both single-quoted ({@code ['key']}) and double-quoted
     * ({@code ["key"]}) forms are supported. The opening quote character determines which quote
     * terminates the key. Called with {@code pos} pointing to the opening quote character.
     * The key may be empty ({@code ['']}) — per RFC 9535, an empty string is a valid member name.
     * <p>
     * Escape sequences inside the quoted string:
     * <ul>
     *   <li>{@code \'} → literal single quote (useful in single-quoted keys: {@code ['it\\'s']})</li>
     *   <li>{@code \"} → literal double quote (useful in double-quoted keys: {@code ["say \\"hi\\""]})</li>
     *   <li>{@code \\} → literal backslash ({@code ['a\\\\b']} accesses key {@code a\b})</li>
     *   <li>Any other character after a backslash is taken literally</li>
     * </ul>
     * <p>
     * After the closing quote, the next character must be {@code ]} — anything else
     * (e.g., {@code ['key'stuff]}) is an error.
     *
     * @param bracketOffset position of the opening {@code [} for error reporting only
     * @return the position immediately after the closing {@code ]}
     */
    private static int readQuotedKey(String path, String originalPath, int pos, int bracketOffset, List<Segment> segments) {
        char quote = path.charAt(pos); // remember whether it's ' or "
        pos++; // skip past the opening quote
        StringBuilder key = new StringBuilder();
        while (pos < path.length()) {
            char c = path.charAt(pos);
            if (c == BACKSLASH && pos + 1 < path.length()) {
                // Escape sequence: take the next character literally
                key.append(path.charAt(pos + 1));
                pos += 2;
            } else if (c == quote) {
                // Closing quote found — skip optional whitespace, then expect ']'
                int afterQuote = skipBlankSpace(path, pos + 1);
                if (afterQuote >= path.length() || path.charAt(afterQuote) != CLOSE_BRACKET) {
                    throw invalidPath(originalPath, "expected closing bracket after quoted key", bracketOffset);
                }
                segments.add(new Segment.Key(key.toString()));
                return afterQuote + 1; // skip past ']'
            } else {
                // Regular character — add to the key
                key.append(c);
                pos++;
            }
        }
        // Reached end of string without finding the closing quote
        throw invalidPath(originalPath, "unterminated quoted key", bracketOffset);
    }

    /**
     * Reads a non-negative integer array index inside brackets: {@code [0]}, {@code [42]}.
     * Called with {@code pos} pointing to the first character after {@code [}.
     * <p>
     * Scans forward to find the closing {@code ]}, then parses the content between the brackets
     * as an integer via {@link Integer#parseInt}. Rejects:
     * <ul>
     *   <li>Leading zeros ({@code [01]}, {@code [007]}) — RFC 9535 explicitly disallows
     *       octal-like integers with leading zeros in array indices</li>
     *   <li>Negative indices ({@code [-1]}) — produces "array index out of bounds"</li>
     *   <li>Non-numeric content ({@code [abc]}, {@code [0:3]}, {@code [*]}, {@code [0,1]}) —
     *       produces "expected integer array index, got [...]". This is how unsupported JSONPath
     *       features like slicing, wildcards, and unions are caught.</li>
     *   <li>Missing closing bracket ({@code [0}) — produces "missing closing bracket"</li>
     * </ul>
     *
     * @param bracketOffset position of the opening {@code [} for error reporting only
     * @return the position immediately after the closing {@code ]}
     */
    private static int readIndex(String path, String originalPath, int pos, int bracketOffset, List<Segment> segments) {
        int end = path.indexOf(CLOSE_BRACKET, pos);
        if (end == -1) {
            throw invalidPath(originalPath, "missing closing bracket for array index", bracketOffset);
        }
        // Trim trailing blank space before "]" (leading blank space already skipped in readBracket).
        // We don't use String.stripTrailing() because it uses Character.isWhitespace(), which
        // includes characters beyond the RFC 9535 B production.
        int contentEnd = end;
        while (contentEnd > pos && isBlank(path.charAt(contentEnd - 1))) {
            contentEnd--;
        }
        String content = path.substring(pos, contentEnd);
        if (content.isEmpty()) {
            throw invalidPath(originalPath, "empty array index", bracketOffset);
        }
        int index;
        try {
            index = Integer.parseInt(content);
        } catch (NumberFormatException e) {
            throw invalidPath(originalPath, "expected integer array index, got [" + content + "]", bracketOffset);
        }
        // RFC 9535 disallows leading zeros in array indices (e.g., [01], [007]) to avoid
        // ambiguity with octal notation. Only bare "0" is valid for index zero.
        // This check comes after parseInt so that non-numeric content like "0:3" gets
        // the more appropriate "expected integer array index" error.
        if (content.length() > 1 && content.charAt(0) == '0') {
            throw invalidPath(originalPath, "leading zeros are not allowed in array index [" + content + "]", bracketOffset);
        }
        if (index < 0) {
            throw new IllegalArgumentException("array index out of bounds");
        }
        segments.add(new Segment.Index(index));
        return end + 1; // skip past the ']'
    }

    // --- Whitespace helper ---

    /**
     * Returns {@code true} if {@code c} is a blank character as defined by RFC 9535:
     * space ({@code %x20}), horizontal tab ({@code %x09}), newline ({@code %x0A}),
     * or carriage return ({@code %x0D}). This matches the RFC's {@code B} production exactly —
     * we intentionally do not use {@code Character.isWhitespace()} because it includes
     * additional characters (form feed, vertical tab) not in the RFC.
     */
    private static boolean isBlank(char c) {
        return c == ' ' || c == '\t' || c == '\n' || c == '\r';
    }

    /**
     * Skips optional blank space starting at {@code pos}.
     * Per RFC 9535, optional blank space ({@code S = *B}) is allowed inside bracketed selections
     * between {@code [} and the selector, and between the selector and {@code ]}.
     *
     * @return the position of the first non-blank character, or {@code path.length()} if
     *         the remainder is all blank space
     */
    private static int skipBlankSpace(String path, int pos) {
        while (pos < path.length() && isBlank(path.charAt(pos))) {
            pos++;
        }
        return pos;
    }

    // --- Error helpers ---

    /** Builds an error for a malformed path without a specific position. */
    private static IllegalArgumentException invalidPath(String path, String reason) {
        return new IllegalArgumentException("Invalid JSON path [" + path + "]: " + reason);
    }

    /** Builds an error for a malformed path, pointing to a specific character position. */
    private static IllegalArgumentException invalidPath(String path, String reason, int position) {
        return new IllegalArgumentException("Invalid JSON path [" + path + "]: " + reason + " at position " + position);
    }
}
