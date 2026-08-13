/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.versionfield;

import org.apache.lucene.index.Term;
import org.apache.lucene.search.AutomatonQuery;
import org.apache.lucene.search.WildcardQuery;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.UnicodeUtil;
import org.apache.lucene.util.automaton.Automata;
import org.apache.lucene.util.automaton.Automaton;
import org.apache.lucene.util.automaton.Operations;
import org.apache.lucene.util.automaton.UTF32ToUTF8;

import java.util.ArrayList;
import java.util.List;

/**
 * A variation of the {@link WildcardQuery} than skips over meta characters introduced using {@link VersionEncoder}.
 * Raw byte-level automaton (marker bytes aren't valid codepoints) — built isBinary=true, skipping Lucene's implicit UTF32ToUTF8.
 */
class VersionFieldWildcardQuery extends AutomatonQuery {

    private static final Automaton OPTIONAL_NUMERIC_CHARPREFIX = Operations.optional(
        Operations.concatenate(Automata.makeChar(VersionEncoder.NUMERIC_MARKER_BYTE), Automata.makeCharRange(0x80, 0xFF))
    );

    private static final Automaton OPTIONAL_RELEASE_SEPARATOR = Operations.optional(
        Operations.union(
            Automata.makeChar(VersionEncoder.PRERELEASE_SEPARATOR_BYTE),
            Automata.makeChar(VersionEncoder.NO_PRERELEASE_SEPARATOR_BYTE)
        )
    );

    // '?' as UTF-8 bytes: one full char, not one raw byte
    private static final Automaton ANY_UTF8_CHAR = new UTF32ToUTF8().convert(Automata.makeAnyChar());

    private static final byte WILDCARD_STRING = '*';

    private static final byte WILDCARD_CHAR = '?';

    VersionFieldWildcardQuery(Term term, boolean caseInsensitive) {
        super(term, toAutomaton(term, caseInsensitive), true);
    }

    VersionFieldWildcardQuery(Term term, boolean caseInsensitive, RewriteMethod rewriteMethod) {
        super(term, toAutomaton(term, caseInsensitive), true, rewriteMethod);
    }

    private static Automaton toAutomaton(Term wildcardquery, boolean caseInsensitive) {
        List<Automaton> automata = new ArrayList<>();

        BytesRef wildcardText = wildcardquery.bytes();
        boolean containsPreReleaseSeparator = false;
        UnicodeUtil.UTF8CodePoint reusableCodePoint = new UnicodeUtil.UTF8CodePoint();
        UTF32ToUTF8 utf32ToUtf8 = new UTF32ToUTF8();

        for (int i = 0; i < wildcardText.length;) {
            final byte c = wildcardText.bytes[wildcardText.offset + i];
            // set below for multi-byte chars
            int length = 1;

            switch (c) {
                case WILDCARD_STRING:
                    automata.add(Automata.makeAnyString());
                    break;
                case WILDCARD_CHAR:
                    // this should also match leading digits, which have optional leading numeric marker and length bytes
                    automata.add(OPTIONAL_NUMERIC_CHARPREFIX);
                    automata.add(OPTIONAL_RELEASE_SEPARATOR);
                    automata.add(ANY_UTF8_CHAR);
                    break;

                case '-':
                    // this should potentially match the first prerelease-dash, so we need an optional marker byte here
                    automata.add(Operations.optional(Automata.makeChar(VersionEncoder.PRERELEASE_SEPARATOR_BYTE)));
                    containsPreReleaseSeparator = true;
                    automata.add(Automata.makeChar(c));
                    break;
                case '+':
                    // this can potentially appear after major version, optionally match the no-prerelease marker
                    automata.add(Operations.optional(Automata.makeChar(VersionEncoder.NO_PRERELEASE_SEPARATOR_BYTE)));
                    containsPreReleaseSeparator = true;
                    automata.add(Automata.makeChar(c));
                    break;
                case '0':
                case '1':
                case '2':
                case '3':
                case '4':
                case '5':
                case '6':
                case '7':
                case '8':
                case '9':
                    boolean firstDigitInGroup = true;
                    if (i > 0
                        && wildcardText.bytes[wildcardText.offset + i - 1] >= (byte) '0'
                        && wildcardText.bytes[wildcardText.offset + i - 1] <= (byte) '9') {
                        firstDigitInGroup = false;
                    }
                    if (firstDigitInGroup) {
                        automata.add(OPTIONAL_NUMERIC_CHARPREFIX);
                    }
                    automata.add(Automata.makeChar(c));
                    break;
                default:
                    if (c >= 0) {
                        // ASCII: byte value == codepoint. Still needs UTF32ToUTF8 conversion when case-insensitive, since
                        // Lucene's Unicode case folding can map an ASCII char to multi-byte equivalents (e.g. 'K' <-> KELVIN SIGN).
                        automata.add(caseInsensitive ? utf32ToUtf8.convert(Automata.makeCaseInsensitiveChar(c)) : Automata.makeChar(c));
                    } else {
                        // multi-byte lead byte: decode full codepoint
                        UnicodeUtil.codePointAt(wildcardText.bytes, wildcardText.offset + i, reusableCodePoint);
                        length = reusableCodePoint.numBytes;
                        int codepoint = reusableCodePoint.codePoint;
                        Automaton charAutomaton = caseInsensitive
                            ? Automata.makeCaseInsensitiveChar(codepoint)
                            : Automata.makeChar(codepoint);
                        // convert codepoint automaton to UTF-8 bytes
                        automata.add(utf32ToUtf8.convert(charAutomaton));
                    }
            }
            i += length;
        }
        // when we only have main version part, we need to add an optional NO_PRERELESE_SEPARATOR_BYTE
        if (containsPreReleaseSeparator == false) {
            automata.add(Operations.optional(Automata.makeChar(VersionEncoder.NO_PRERELEASE_SEPARATOR_BYTE)));
        }
        return Operations.determinize(Operations.concatenate(automata), Operations.DEFAULT_DETERMINIZE_WORK_LIMIT);
    }

    @Override
    public String toString(String field) {
        StringBuilder buffer = new StringBuilder();
        if (getField().equals(field) == false) {
            buffer.append(getField());
            buffer.append(":");
        }
        buffer.append(term.text());
        return buffer.toString();
    }
}
