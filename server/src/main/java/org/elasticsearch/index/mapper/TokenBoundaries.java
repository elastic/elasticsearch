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
 * Whether a stretch of a value is a whole token of it, read from the bytes on either side. Some analyzers cut
 * their input into runs of word characters and lowercase them, so a run that spells a term <em>is</em> that term's
 * token, and a value holding one answers a query for that term without being analyzed.
 *
 * <p>The answer is one of three: the stretch is a token, it is part of a longer one, or the bytes around it do not
 * say. Only ASCII is read, so a neighbour outside ASCII leaves the question open and the value is analyzed as it
 * was. A byte that joins two runs into one token leaves it open as well, which keeps this narrower than the
 * analyzer rather than wider.
 */
final class TokenBoundaries {

    /** What the bytes around a stretch of a value say about it. */
    enum Verdict {
        /** The stretch is a token of the value, so an analyzer would leave it whole. */
        TOKEN,
        /** The stretch runs on into a longer token, so no analyzer leaves it as it stands. */
        PART_OF_ONE,
        /** The bytes around it do not say, so only analyzing the value answers. */
        UNKNOWN
    }

    /** What a tokenizer takes to be part of a word, which is the one rule these analyzers differ by. */
    private enum WordBytes {
        /** Letters and digits, which is the run a {@link StandardAnalyzer} leaves whole. */
        LETTERS_AND_DIGITS,
        /** Letters alone, which is all a {@link SimpleAnalyzer} or a {@link StopAnalyzer} keeps. */
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
     * How {@code analyzer} delimits its tokens, or {@code null} where its tokens are not runs of the value that it
     * lowercases. An analyzer that keeps the case of what it reads is left out: a stretch spelling a term in
     * another case is not that term's token, which these read without telling the two apart.
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
     * Whether a stretch of a value spelling {@code term} can be read as its token: the term has to be spelled by
     * the bytes this reads, be a token the analyzer keeps rather than one it drops, and be short enough that the
     * analyzer would not cut it in two.
     */
    boolean canReadTokensOf(BytesRef term) {
        // A term as long as the longest token is left out as well: the analyzer cuts a longer run at that length,
        // which leaves a token the bytes after it say runs on.
        if (term.length == 0 || term.length >= longestToken) {
            return false;
        }
        for (int i = term.offset, to = term.offset + term.length; i < to; i++) {
            final byte b = term.bytes[i];
            // A term spelled with a capital is no token of an analyzer that lowercases what it reads.
            if (isWordByte(b) == false || (b >= 'A' && b <= 'Z')) {
                return false;
            }
        }
        return stopWords.isEmpty() || stopWords.contains(term.utf8ToString()) == false;
    }

    /**
     * What the bytes around the {@code length} bytes of {@code value} at {@code at} say about them, which is read
     * from the two bytes after the stretch and, before it, from the run the stretch continues.
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
     * What the bytes before the stretch starting at {@code at} say. A run the stretch continues makes it part of a
     * longer token only where the run is shorter than the longest token: the analyzer cuts a longer run at that
     * length, and a cut can fall where the stretch starts.
     */
    private Verdict beforeStretch(BytesRef value, int at) {
        if (at == value.offset) {
            // Each value is analyzed on its own, so where one starts a token starts.
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
        // The token holding the byte before the stretch starts no nearer than this, so it is shorter than the
        // longest token.
        return stop == value.offset ? Verdict.PART_OF_ONE : Verdict.UNKNOWN;
    }

    /** What the bytes after the stretch ending at {@code to} say. */
    private Verdict afterStretch(BytesRef value, int to) {
        if (to == value.offset + value.length) {
            return Verdict.TOKEN;
        }
        return neighbour(value, to, to + 1);
    }

    /**
     * What the byte next to a stretch says, where {@code beyond} holds the one after it on the same side. A byte
     * that is part of a word runs the token on; one that can join two words into one is read as saying nothing,
     * unless what lies beyond it is no word either, since then there is nothing for it to join.
     */
    private Verdict neighbour(BytesRef value, int next, int beyond) {
        final byte b = value.bytes[next];
        if (b < 0) {
            // Outside ASCII, where what a character is takes more than a byte to say.
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
     * Whether a byte holds the runs on either side of it together into one token, which an underscore does for a
     * tokenizer that keeps more than letters. A tokenizer keeping letters alone cuts its tokens there instead, so
     * for it an underscore ends a token like any other byte that is no letter.
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
     * Whether a byte can hold the words on either side of it together, which the rules a {@link StandardAnalyzer}
     * follows let these do between two words. A tokenizer that keeps only letters joins nothing.
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
     * Whether the {@code term.length} bytes of {@code value} at {@code at} spell {@code term} once lowercased,
     * which is the comparison the analyzer's own lowercasing comes to for a term of ASCII. Only a letter has another
     * case: the byte a digit differs from by the same bit is a control character.
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
