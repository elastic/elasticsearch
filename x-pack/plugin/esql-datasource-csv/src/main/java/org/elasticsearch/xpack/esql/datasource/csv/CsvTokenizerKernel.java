/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasource.csv;

import org.elasticsearch.core.Nullable;

/**
 * Shared decode kernel for the CSV/TSV tokenizer. Contains the two genuinely complex, drift-prone
 * inner loops that otherwise live in two parallel copies: the quoted-region walk
 * ({@link #decodeQuotedBody}) and the backslash-escaped unquoted walk
 * ({@link #decodeUnquotedEscapedBody}).
 *
 * <p>Each helper has two overloads — one for {@code String} (the house record tokenizer) and one
 * for {@code char[]} (the direct-to-block walker) — so each arm uses direct character access with
 * no intermediate allocation. The zero-alloc plain fast path ({@code splitAndConvertPlain}) is
 * kept separate and does not use this kernel.
 *
 * <p>Error handling (cap enforcement, row-level error routing) stays at each call site because the
 * two arms differ: the house arm throws {@link MalformedRowException}, the walker arm calls its
 * instance-level error policy. The helpers return structural results (decoded lengths, end positions)
 * and only signal unclosed quotes via {@link #UNCLOSED_QUOTED_FIELD}, which both arms handle by
 * throwing with their own record context.
 */
class CsvTokenizerKernel {

    private CsvTokenizerKernel() {}

    /**
     * Sentinel returned by {@link #decodeQuotedBody} when the closing quote is never found.
     * Callers must check for this value and throw an appropriate
     * {@link MalformedRowException#unclosedQuotedField} with their own record context.
     */
    static final long UNCLOSED_QUOTED_FIELD = -1L;

    /**
     * Extracts the decoded-character count from a successful {@link #decodeQuotedBody} result.
     * Used by call sites to enforce {@code maxFieldChars}.
     */
    static int quotedBodyDecodedLen(long result) {
        return (int) (result >>> 32);
    }

    /**
     * Extracts the position immediately after the closing quote from a successful
     * {@link #decodeQuotedBody} result. Call sites use it to scan trailing whitespace and locate
     * the next delimiter.
     */
    static int quotedBodyEndPos(long result) {
        return (int) (result & 0xFFFFFFFFL);
    }

    // -----------------------------------------------------------------------------------------
    // decodeQuotedBody — String and char[] overloads
    // -----------------------------------------------------------------------------------------

    /**
     * Walks a quoted field body in {@code src[from, to)} starting just after the opening quote,
     * decoding RFC 4180 doubled-quote pairs ({@code ""} → literal {@code "}) and, when
     * {@code escapeAware}, backslash sequences via
     * {@link CsvFormatReader#decodeQuotedEscapeChar}. Appends decoded characters to {@code out}
     * when non-null; pass {@code null} to count without appending (for non-projected fields that
     * only need the cap check). Returns {@link #UNCLOSED_QUOTED_FIELD} if the closing quote is
     * never found before {@code to} — callers must throw an appropriate
     * {@link MalformedRowException#unclosedQuotedField} with their own record context.
     *
     * <p>On success, the return value encodes both the decoded-character count (high 32 bits,
     * extract with {@link #quotedBodyDecodedLen}) and the position immediately after the closing
     * quote (low 32 bits, extract with {@link #quotedBodyEndPos}). The packed {@code long}
     * representation avoids per-field object allocation in the large caller methods where JIT
     * escape analysis is unlikely to scalar-replace a record.
     *
     * <p>Does NOT enforce {@code maxFieldChars} or scan content after the closing quote — both
     * remain at the call site where error handling differs between the house and walker arms.
     */
    static long decodeQuotedBody(String src, int from, int to, char quote, char esc, boolean escapeAware, @Nullable StringBuilder out) {
        int q = from;
        int decodedLen = 0;
        while (q < to) {
            char c = src.charAt(q);
            if (c == quote) {
                if (q + 1 < to && src.charAt(q + 1) == quote) {
                    if (out != null) out.append(quote);
                    decodedLen++;
                    q += 2;
                    continue;
                }
                return ((long) decodedLen << 32) | ((long) q + 1);
            }
            if (escapeAware && c == esc) {
                if (q + 1 < to) {
                    if (out != null) out.append(CsvFormatReader.decodeQuotedEscapeChar(src.charAt(q + 1)));
                    decodedLen++;
                    q += 2;
                } else {
                    q++; // trailing lone escape: dropped
                }
                continue;
            }
            if (out != null) out.append(c);
            decodedLen++;
            q++;
        }
        return UNCLOSED_QUOTED_FIELD;
    }

    /**
     * Walks a quoted field body in {@code src[from, to)} starting just after the opening quote.
     * See {@link #decodeQuotedBody(String, int, int, char, char, boolean, StringBuilder)} for full
     * contract.
     */
    static long decodeQuotedBody(char[] src, int from, int to, char quote, char esc, boolean escapeAware, @Nullable StringBuilder out) {
        int q = from;
        int decodedLen = 0;
        while (q < to) {
            char c = src[q];
            if (c == quote) {
                if (q + 1 < to && src[q + 1] == quote) {
                    if (out != null) out.append(quote);
                    decodedLen++;
                    q += 2;
                    continue;
                }
                return ((long) decodedLen << 32) | ((long) q + 1);
            }
            if (escapeAware && c == esc) {
                if (q + 1 < to) {
                    if (out != null) out.append(CsvFormatReader.decodeQuotedEscapeChar(src[q + 1]));
                    decodedLen++;
                    q += 2;
                } else {
                    q++; // trailing lone escape: dropped
                }
                continue;
            }
            if (out != null) out.append(c);
            decodedLen++;
            q++;
        }
        return UNCLOSED_QUOTED_FIELD;
    }

    // -----------------------------------------------------------------------------------------
    // decodeUnquotedEscapedBody — String and char[] overloads
    // -----------------------------------------------------------------------------------------

    /**
     * Decodes a backslash-escaped unquoted field range {@code src[start, end)}: trims leading raw
     * whitespace when {@code trimSpaces}, decodes {@code \c} sequences via
     * {@link CsvFormatReader#decodeQuotedEscapeChar} (a trailing lone escape is dropped), appends
     * decoded characters to {@code out} when non-null (pass {@code null} to count without
     * appending, for non-projected cap checks), and returns the trimmed decoded length (trailing
     * decoded whitespace is deducted only when {@code trimSpaces}).
     *
     * <p>A return value of {@code 0} means the field decoded to empty (whitespace-only under trim,
     * or genuinely empty); call sites should treat this as a present-but-empty field.
     * Does NOT enforce {@code maxFieldChars} — that stays at each call site.
     */
    static int decodeUnquotedEscapedBody(String src, int start, int end, char esc, boolean trimSpaces, @Nullable StringBuilder out) {
        if (trimSpaces) {
            while (start < end && src.charAt(start) <= ' ') {
                start++;
            }
        }
        int decodedLen = 0;
        int trailingWs = 0;
        for (int k = start; k < end; k++) {
            char c = src.charAt(k);
            if (c == esc) {
                if (k + 1 < end) {
                    char decoded = CsvFormatReader.decodeQuotedEscapeChar(src.charAt(++k));
                    if (out != null) out.append(decoded);
                    decodedLen++;
                    if (trimSpaces) trailingWs = decoded <= ' ' ? trailingWs + 1 : 0;
                }
                // else: trailing lone escape, dropped
            } else {
                if (out != null) out.append(c);
                decodedLen++;
                if (trimSpaces) trailingWs = c <= ' ' ? trailingWs + 1 : 0;
            }
        }
        return trimSpaces ? decodedLen - trailingWs : decodedLen;
    }

    /**
     * Decodes a backslash-escaped unquoted field range {@code src[start, end)}.
     * See {@link #decodeUnquotedEscapedBody(String, int, int, char, boolean, StringBuilder)} for
     * full contract.
     */
    static int decodeUnquotedEscapedBody(char[] src, int start, int end, char esc, boolean trimSpaces, @Nullable StringBuilder out) {
        if (trimSpaces) {
            while (start < end && src[start] <= ' ') {
                start++;
            }
        }
        int decodedLen = 0;
        int trailingWs = 0;
        for (int k = start; k < end; k++) {
            char c = src[k];
            if (c == esc) {
                if (k + 1 < end) {
                    char decoded = CsvFormatReader.decodeQuotedEscapeChar(src[++k]);
                    if (out != null) out.append(decoded);
                    decodedLen++;
                    if (trimSpaces) trailingWs = decoded <= ' ' ? trailingWs + 1 : 0;
                }
                // else: trailing lone escape, dropped
            } else {
                if (out != null) out.append(c);
                decodedLen++;
                if (trimSpaces) trailingWs = c <= ' ' ? trailingWs + 1 : 0;
            }
        }
        return trimSpaces ? decodedLen - trailingWs : decodedLen;
    }

    // -----------------------------------------------------------------------------------------
    // scanUnquotedField — String and char[] overloads
    // -----------------------------------------------------------------------------------------

    /**
     * Bit flag set in the return value of {@link #scanUnquotedField} when at least one escape
     * character was found in the unquoted field span. Callers test with
     * {@link #scanHasEscape(long)}.
     */
    private static final long HAS_ESCAPE = 1L << 32;

    /** True when the {@link #scanUnquotedField} result indicates at least one escape character. */
    static boolean scanHasEscape(long result) {
        return (result & HAS_ESCAPE) != 0;
    }

    /** Extracts the field-end position from a {@link #scanUnquotedField} result. */
    static int scanFieldEnd(long result) {
        return (int) (result & 0xFFFFFFFFL);
    }

    /**
     * Scans an unquoted field in {@code src[from, to)} to its end delimiter, skipping
     * {@code esc+any-char} pairs when {@code escapeAware} (so an escaped delimiter is not treated
     * as a field boundary). Returns a packed value: bit 32 is set if any escape character was seen
     * (test with {@link #scanHasEscape}); the low 32 bits hold the field-end position — the index of
     * the delimiter that ended the scan, or {@code to} if the scan reached the end without finding
     * one (extract with {@link #scanFieldEnd}).
     *
     * <p>The plain (no-escape) fast path is not routed here — callers that already know there are
     * no escapes use a direct delimiter scan.
     */
    static long scanUnquotedField(String src, int from, int to, char delim, char esc, boolean escapeAware) {
        int j = from;
        long hasEsc = 0;
        while (j < to) {
            char c = src.charAt(j);
            if (escapeAware && c == esc) {
                hasEsc = HAS_ESCAPE;
                j += 2; // skip the escaped char (even if it is a delimiter)
                continue;
            }
            if (c == delim) {
                break;
            }
            j++;
        }
        return hasEsc | (long) Math.min(j, to);
    }

    /**
     * Scans an unquoted field in {@code src[from, to)}.
     * See {@link #scanUnquotedField(String, int, int, char, char, boolean)} for full contract.
     */
    static long scanUnquotedField(char[] src, int from, int to, char delim, char esc, boolean escapeAware) {
        int j = from;
        long hasEsc = 0;
        while (j < to) {
            char c = src[j];
            if (escapeAware && c == esc) {
                hasEsc = HAS_ESCAPE;
                j += 2;
                continue;
            }
            if (c == delim) {
                break;
            }
            j++;
        }
        return hasEsc | (long) Math.min(j, to);
    }
}
