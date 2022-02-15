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
import org.apache.lucene.util.automaton.Automata;
import org.apache.lucene.util.automaton.Automaton;
import org.apache.lucene.util.automaton.Operations;
import org.elasticsearch.common.lucene.search.AutomatonQueries;

import java.util.ArrayList;
import java.util.List;

/**
 * A variation of the {@link WildcardQuery} than skips over meta characters introduced using {@link VersionEncoder}.
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

    private static final byte WILDCARD_STRING = '*';

    private static final byte WILDCARD_CHAR = '?';

    VersionFieldWildcardQuery(Term term, boolean caseInsensitive) {
        super(term, toAutomaton(term, caseInsensitive), Integer.MAX_VALUE, true);
    }

    private static Automaton toAutomaton(Term wildcardquery, boolean caseInsensitive) {
        List<Automaton> automata = new ArrayList<>();

        BytesRef wildcardText = wildcardquery.bytes();
        boolean containsPreReleaseSeparator = false;

        for (int i = 0; i < wildcardText.length;) {
            final byte c = wildcardText.bytes[wildcardText.offset + i];
            int length = Character.charCount(c);

            switch (c) {
                case WILDCARD_STRING:
                    automata.add(Automata.makeAnyString());
                    break;
                case WILDCARD_CHAR:
                    // this should also match leading digits, which have optional leading numeric marker and length bytes
                    automata.add(OPTIONAL_NUMERIC_CHARPREFIX);
                    automata.add(OPTIONAL_RELEASE_SEPARATOR);
                    automata.add(Automata.makeAnyChar());
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
                    if (caseInsensitive == false) {
                        automata.add(Automata.makeChar(c));
                    } else {
                        automata.add(AutomatonQueries.toCaseInsensitiveChar(c, Integer.MAX_VALUE));
                    }
            }
            i += length;
        }
        // when we only have main version part, we need to add an optional NO_PRERELESE_SEPARATOR_BYTE
        if (containsPreReleaseSeparator == false) {
            automata.add(Operations.optional(Automata.makeChar(VersionEncoder.NO_PRERELEASE_SEPARATOR_BYTE)));
        }
        return Operations.concatenate(automata);
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
