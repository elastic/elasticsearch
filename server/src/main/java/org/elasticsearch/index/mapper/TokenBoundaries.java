/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.mapper;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.CharArraySet;
import org.apache.lucene.analysis.core.SimpleAnalyzer;
import org.apache.lucene.analysis.core.StopAnalyzer;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.analysis.util.CharTokenizer;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.index.analysis.NamedAnalyzer;

/**
 * Decides whether a substring of a value is a whole token, by looking at the bytes on either side. The supported
 * analyzers split their input into runs of word characters and lowercase them, so a run that spells a term is that
 * term's token, and the value matches a query for the term without being analyzed.
 *
 * <p>There are three outcomes: the substring is a token, it is part of a longer token, or the neighboring bytes are
 * not conclusive. Only ASCII neighbors are classified; anything else is inconclusive and the value is analyzed. So
 * are punctuation characters that the tokenizer may or may not split on, which keeps the check conservative.
 */
final class TokenBoundaries {

    /** The outcome of checking a substring. */
    enum Verdict {
        /** The substring is a whole token. */
        TOKEN,
        /** The substring is part of a longer token. */
        PART_OF_ONE,
        /** The neighboring bytes are not conclusive. */
        UNKNOWN
    }

    /** Which ASCII characters a tokenizer treats as word characters. */
    private enum WordBytes {
        /** Letters and digits, as in {@link StandardAnalyzer}. */
        LETTERS_AND_DIGITS,
        /** Letters only, as in {@link SimpleAnalyzer} and {@link StopAnalyzer}. */
        LETTERS
    }

    private final WordBytes wordBytes;
    private final CharArraySet stopWords;
    private final int longestToken;

    private TokenBoundaries(WordBytes wordBytes, CharArraySet stopWords, int longestToken) {
        this.wordBytes = wordBytes;
        this.stopWords = stopWords;
        this.longestToken = longestToken;
    }

    /**
     * The token boundaries of {@code analyzer}, or {@code null} if it is not a lowercasing analyzer whose tokens are
     * runs of word characters. Analyzers that preserve case are excluded, since the needle search ignores case and
     * cannot tell {@code Cat} from {@code cat}.
     */
    @Nullable
    static TokenBoundaries of(@Nullable Analyzer analyzer) {
        Analyzer unwrapped = analyzer;
        while (unwrapped instanceof NamedAnalyzer named) {
            unwrapped = named.analyzer();
        }
        if (unwrapped instanceof StandardAnalyzer standard) {
            return new TokenBoundaries(WordBytes.LETTERS_AND_DIGITS, standard.getStopwordSet(), standard.getMaxTokenLength());
        }
        if (unwrapped instanceof StopAnalyzer stop) {
            return new TokenBoundaries(WordBytes.LETTERS, stop.getStopwordSet(), CharTokenizer.DEFAULT_MAX_WORD_LEN);
        }
        if (unwrapped instanceof SimpleAnalyzer) {
            return new TokenBoundaries(WordBytes.LETTERS, CharArraySet.EMPTY_SET, CharTokenizer.DEFAULT_MAX_WORD_LEN);
        }
        return null;
    }

    /**
     * Whether an occurrence of {@code term} can be recognized as a token: the term must consist of lowercase ASCII
     * word characters, must not be a stop word, and must be shorter than the maximum token length.
     */
    boolean canReadTokensOf(BytesRef term) {
        // The analyzer splits longer runs at the maximum token length, so a term of that length can be a token
        // followed by a word character, which the end boundary check would wrongly reject.
        if (term.length == 0 || term.length >= longestToken) {
            return false;
        }
        for (int i = term.offset, to = term.offset + term.length; i < to; i++) {
            final byte b = term.bytes[i];
            // A lowercasing analyzer never produces a token with an uppercase letter.
            if (isWordByte(b) == false || (b >= 'A' && b <= 'Z')) {
                return false;
            }
        }
        return stopWords.isEmpty() || stopWords.contains(term.utf8ToString()) == false;
    }

    /**
     * Checks whether the {@code length} bytes of {@code value} at {@code at} are a whole token. Looks at up to two
     * bytes after the substring and, before it, at the run of word characters it may continue.
     */
    Verdict verdictAt(BytesRef value, int at, int length) {
        final Verdict before = beforeStretch(value, at);
        if (before == Verdict.PART_OF_ONE) {
            return Verdict.PART_OF_ONE;
        }
        final Verdict after = afterStretch(value, at + length);
        if (after == Verdict.PART_OF_ONE) {
            return Verdict.PART_OF_ONE;
        }
        return before == Verdict.TOKEN && after == Verdict.TOKEN ? Verdict.TOKEN : Verdict.UNKNOWN;
    }

    /**
     * Checks the start boundary of the substring at {@code at}. A preceding word character means the substring is
     * part of a longer token only if the run before it is shorter than the maximum token length; otherwise the
     * analyzer may have split the run exactly where the substring starts.
     */
    private Verdict beforeStretch(BytesRef value, int at) {
        if (at == value.offset) {
            // Each value is analyzed separately, so a token starts at the start of a value.
            return Verdict.TOKEN;
        }
        final Verdict verdict = neighbour(value, at - 1, at - 2);
        if (verdict != Verdict.PART_OF_ONE) {
            return verdict;
        }
        final int stop = Math.max(value.offset, at - longestToken + 1);
        for (int i = at - 1; i >= stop; i--) {
            final byte b = value.bytes[i];
            if (b < 0) {
                return Verdict.UNKNOWN;
            }
            if (isWordByte(b) == false && holdsRunsTogether(b) == false && joinsWords(b) == false) {
                return Verdict.PART_OF_ONE;
            }
        }
        // The run reaches the start of the value within the maximum token length, so it is not split.
        return stop == value.offset ? Verdict.PART_OF_ONE : Verdict.UNKNOWN;
    }

    /** Checks the end boundary of the substring ending at {@code to}. */
    private Verdict afterStretch(BytesRef value, int to) {
        if (to == value.offset + value.length) {
            return Verdict.TOKEN;
        }
        return neighbour(value, to, to + 1);
    }

    /**
     * Classifies the byte at {@code next}, adjacent to the substring; {@code beyond} is the following byte on the
     * same side. A word character continues the token. A character that can join two words (see {@link #joinsWords})
     * is inconclusive, unless the byte beyond it is not a word character, in which case there is nothing to join.
     */
    private Verdict neighbour(BytesRef value, int next, int beyond) {
        final byte b = value.bytes[next];
        if (b < 0) {
            // Part of a multi-byte character, which is not classified.
            return Verdict.UNKNOWN;
        }
        if (isWordByte(b) || holdsRunsTogether(b)) {
            return Verdict.PART_OF_ONE;
        }
        if (joinsWords(b)) {
            final boolean inValue = beyond >= value.offset && beyond < value.offset + value.length;
            if (inValue == false) {
                return Verdict.TOKEN;
            }
            final byte far = value.bytes[beyond];
            return far < 0 || isWordByte(far) || holdsRunsTogether(far) ? Verdict.UNKNOWN : Verdict.TOKEN;
        }
        return Verdict.TOKEN;
    }

    /**
     * Whether the byte always joins the runs on either side into one token: an underscore under
     * {@link StandardAnalyzer}. Letter tokenizers split on it like on any other non-letter.
     */
    private boolean holdsRunsTogether(byte b) {
        return b == '_' && wordBytes == WordBytes.LETTERS_AND_DIGITS;
    }

    private boolean isWordByte(byte b) {
        if (b < 0) {
            return false;
        }
        final boolean letter = (b | 0x20) >= 'a' && (b | 0x20) <= 'z';
        return letter || (wordBytes == WordBytes.LETTERS_AND_DIGITS && b >= '0' && b <= '9');
    }

    /**
     * Whether the byte may join the words on either side into one token under the Unicode word break rules that
     * {@link StandardAnalyzer} follows (for example {@code 3.14} or {@code can't}). Letter tokenizers join nothing.
     */
    private boolean joinsWords(byte b) {
        if (wordBytes == WordBytes.LETTERS) {
            return false;
        }
        return switch (b) {
            case '.', ',', ':', ';', '\'', '"' -> true;
            default -> false;
        };
    }

    /**
     * Whether the {@code term.length} bytes of {@code value} at {@code at} equal {@code term} once their ASCII
     * letters are lowercased. Only letters are compared ignoring case: flipping the case bit of a digit gives a
     * control character.
     */
    static boolean spellsTerm(BytesRef value, int at, BytesRef term) {
        for (int i = 0; i < term.length; i++) {
            final byte of = value.bytes[at + i];
            final byte asked = term.bytes[term.offset + i];
            if (of != asked && (asked < 'a' || asked > 'z' || of != asked - ('a' - 'A'))) {
                return false;
            }
        }
        return true;
    }
}
