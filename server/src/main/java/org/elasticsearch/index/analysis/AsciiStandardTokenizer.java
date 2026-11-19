/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.analysis;

import org.apache.lucene.analysis.LowerCaseFilter;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.Tokenizer;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.analysis.standard.StandardTokenizer;
import org.apache.lucene.analysis.tokenattributes.CharTermAttribute;
import org.apache.lucene.analysis.tokenattributes.OffsetAttribute;
import org.apache.lucene.analysis.tokenattributes.PositionIncrementAttribute;
import org.apache.lucene.analysis.tokenattributes.TypeAttribute;
import org.apache.lucene.util.Attribute;
import org.apache.lucene.util.AttributeFactory;
import org.apache.lucene.util.AttributeImpl;

import java.io.CharArrayReader;
import java.io.IOException;
import java.io.Reader;
import java.util.Arrays;

/**
 * A drop-in replacement for the chain {@code StandardTokenizer -> LowerCaseFilter} that is much cheaper for pure ASCII text, which is
 * what log messages almost always contain.
 * <p>
 * The whole value is buffered first. If every char is below {@code 0x80} we tokenize with a small hand written state machine
 * (see {@link #TRANSITIONS}) and lower case while we copy the term, in a single pass and without any per-token virtual calls into
 * other stream stages. If we see any other char, or the value is larger than {@link #maxBufferedChars}, we hand the <em>whole</em>
 * value to a regular {@link StandardTokenizer} followed by a {@link LowerCaseFilter}. We never mix both paths inside one value
 * because a non-ASCII char (for example a combining mark) can extend the ASCII token in front of it.
 * <p>
 * Both paths must emit identical terms, offsets, position increments and types. {@code AsciiStandardAnalyzerTests} verifies this
 * against {@link StandardAnalyzer}, and it must keep passing when Lucene updates the Unicode version of the {@code StandardTokenizer}
 * grammar.
 *
 * <h2>The ASCII grammar</h2>
 * The {@code StandardTokenizer} grammar (Unicode 12.1 word break rules, longest match) reduces for ASCII input to this, with
 * {@code L} = letter, {@code N} = digit, {@code _} = ExtendNumLet:
 * <pre>
 *   token  := _* A (sep A)* _*            A is a letter or a digit
 *   sep    := (empty) | _+ | m            m joins two letters if it is one of  : . '
 *                                         and joins two digits if it is one of  , ; . '
 * </pre>
 * The type is {@code <NUM>} if the token contains no letter and {@code <ALPHANUM>} otherwise. A token is never longer than
 * {@code max_token_length}: the JFlex scanner of Lucene has no buffer expansion, so it behaves as if the input ended
 * {@code max_token_length} chars after the start of every match attempt, and we do the same.
 */
public final class AsciiStandardTokenizer extends Tokenizer {

    /** Values larger than this (in chars) are not buffered and use the regular tokenizer. */
    static final int DEFAULT_MAX_BUFFERED_CHARS = 1 << 20;
    private static final int INITIAL_BUFFER_CHARS = 1 << 10;
    /** We do not keep a buffer larger than this between values, to avoid pinning memory for one outlier. */
    private static final int MAX_RETAINED_BUFFER_CHARS = 1 << 16;

    private static final byte C_OTHER = 0;
    private static final byte C_LETTER = 1;
    private static final byte C_DIGIT = 2;
    private static final byte C_UNDERSCORE = 3;
    /** {@code .} and {@code '} join letters and digits. */
    private static final byte C_JOIN_ANY = 4;
    /** {@code :} joins letters only. */
    private static final byte C_JOIN_LETTER = 5;
    /** {@code ,} and {@code ;} join digits only. */
    private static final byte C_JOIN_DIGIT = 6;
    private static final int CLASS_SHIFT = 3;

    // States, accepting states must stay last so that one comparison tells us if a state accepts.
    private static final byte DEAD = 0;
    private static final byte START = 1;
    private static final byte LEADING_UNDERSCORES = 2;
    private static final byte AWAIT_LETTER = 3;
    private static final byte AWAIT_DIGIT = 4;
    private static final byte AFTER_LETTER = 5;
    private static final byte AFTER_DIGIT = 6;
    private static final byte AFTER_UNDERSCORES = 7;
    private static final int FIRST_ACCEPTING = AFTER_LETTER;
    private static final int STATE_COUNT = 8;

    /** Class of every ASCII char. */
    private static final byte[] CLASSES = new byte[128];
    /** Lower case version of every ASCII char. */
    private static final char[] LOWER = new char[128];
    /** Next state, indexed by {@code (state << CLASS_SHIFT) | class}. */
    private static final byte[] TRANSITIONS = new byte[STATE_COUNT << CLASS_SHIFT];

    static {
        for (char c = 0; c < 128; c++) {
            LOWER[c] = c >= 'A' && c <= 'Z' ? (char) (c + ('a' - 'A')) : c;
            if (c >= 'a' && c <= 'z' || c >= 'A' && c <= 'Z') {
                CLASSES[c] = C_LETTER;
            } else if (c >= '0' && c <= '9') {
                CLASSES[c] = C_DIGIT;
            } else {
                CLASSES[c] = switch (c) {
                    case '_' -> C_UNDERSCORE;
                    case '.', '\'' -> C_JOIN_ANY;
                    case ':' -> C_JOIN_LETTER;
                    case ',', ';' -> C_JOIN_DIGIT;
                    default -> C_OTHER;
                };
            }
        }
        // Everything not listed below goes to DEAD (0).
        for (byte state : new byte[] { START, LEADING_UNDERSCORES, AFTER_LETTER, AFTER_DIGIT, AFTER_UNDERSCORES }) {
            transition(state, C_LETTER, AFTER_LETTER);
            transition(state, C_DIGIT, AFTER_DIGIT);
        }
        transition(START, C_UNDERSCORE, LEADING_UNDERSCORES);
        transition(LEADING_UNDERSCORES, C_UNDERSCORE, LEADING_UNDERSCORES);
        for (byte state : new byte[] { AFTER_LETTER, AFTER_DIGIT, AFTER_UNDERSCORES }) {
            transition(state, C_UNDERSCORE, AFTER_UNDERSCORES);
        }
        transition(AFTER_LETTER, C_JOIN_ANY, AWAIT_LETTER);
        transition(AFTER_LETTER, C_JOIN_LETTER, AWAIT_LETTER);
        transition(AFTER_DIGIT, C_JOIN_ANY, AWAIT_DIGIT);
        transition(AFTER_DIGIT, C_JOIN_DIGIT, AWAIT_DIGIT);
        // A joiner is only valid if the next char has the same type as the char before the joiner.
        transition(AWAIT_LETTER, C_LETTER, AFTER_LETTER);
        transition(AWAIT_DIGIT, C_DIGIT, AFTER_DIGIT);
    }

    private static void transition(byte from, byte charClass, byte to) {
        TRANSITIONS[(from << CLASS_SHIFT) | charClass] = to;
    }

    /**
     * Gives the interfaces that Lucene's default factory implements with one {@code PackedTokenAttributeImpl} to
     * {@link AsciiTermAttributeImpl} instead, which is the same object with a faster {@code getBytesRef()} for ASCII terms.
     */
    private static final AttributeFactory ATTRIBUTE_FACTORY = new AttributeFactory() {
        @Override
        public AttributeImpl createAttributeInstance(Class<? extends Attribute> attClass) {
            return attClass.isAssignableFrom(AsciiTermAttributeImpl.class)
                ? new AsciiTermAttributeImpl()
                : DEFAULT_ATTRIBUTE_FACTORY.createAttributeInstance(attClass);
        }
    };

    private final CharTermAttribute termAtt = addAttribute(CharTermAttribute.class);
    private final OffsetAttribute offsetAtt = addAttribute(OffsetAttribute.class);
    private final PositionIncrementAttribute posIncAtt = addAttribute(PositionIncrementAttribute.class);
    private final TypeAttribute typeAtt = addAttribute(TypeAttribute.class);

    private int maxTokenLength = StandardAnalyzer.DEFAULT_MAX_TOKEN_LENGTH;
    private final int maxBufferedChars;

    private char[] buffer = new char[INITIAL_BUFFER_CHARS];
    private int length;
    private int position;

    // Lazily created, only used for values that are not pure ASCII.
    private StandardTokenizer fallbackSource;
    private TokenStream fallbackChain;
    private CharTermAttribute fallbackTerm;
    private OffsetAttribute fallbackOffset;
    private PositionIncrementAttribute fallbackPosInc;
    private TypeAttribute fallbackType;
    private boolean usingFallback;

    public AsciiStandardTokenizer() {
        this(DEFAULT_MAX_BUFFERED_CHARS);
    }

    AsciiStandardTokenizer(int maxBufferedChars) {
        super(ATTRIBUTE_FACTORY);
        this.maxBufferedChars = maxBufferedChars;
    }

    /**
     * Same contract and error messages as {@link StandardTokenizer#setMaxTokenLength}.
     */
    public void setMaxTokenLength(int length) {
        if (length < 1) {
            throw new IllegalArgumentException("maxTokenLength must be greater than zero");
        } else if (length > StandardTokenizer.MAX_TOKEN_LENGTH_LIMIT) {
            throw new IllegalArgumentException("maxTokenLength may not exceed " + StandardTokenizer.MAX_TOKEN_LENGTH_LIMIT);
        }
        this.maxTokenLength = length;
    }

    public int getMaxTokenLength() {
        return maxTokenLength;
    }

    @Override
    public void reset() throws IOException {
        super.reset();
        usingFallback = false;
        position = 0;
        length = 0;

        char[] buf = buffer;
        int n = 0;
        while (true) {
            if (n >= maxBufferedChars) {
                // Too large to buffer. The fallback reads what we already have followed by the rest of the input.
                startFallback(new PrefixReader(new CharArrayReader(buf, 0, n), input));
                return;
            }
            if (n == buf.length) {
                buf = buffer = Arrays.copyOf(buf, (int) Math.min((long) maxBufferedChars, 2L * buf.length));
            }
            int read = input.read(buf, n, Math.min(buf.length, maxBufferedChars) - n);
            if (read < 0) {
                break;
            }
            n += read;
        }

        int bits = 0;
        for (int i = 0; i < n; i++) {
            bits |= buf[i];
        }
        if (bits >= 128) {
            startFallback(new CharArrayReader(buf, 0, n));
            return;
        }
        length = n;
    }

    private void startFallback(Reader reader) throws IOException {
        if (fallbackSource == null) {
            fallbackSource = new StandardTokenizer();
            fallbackChain = new LowerCaseFilter(fallbackSource);
            fallbackTerm = fallbackChain.getAttribute(CharTermAttribute.class);
            fallbackOffset = fallbackChain.getAttribute(OffsetAttribute.class);
            fallbackPosInc = fallbackChain.getAttribute(PositionIncrementAttribute.class);
            fallbackType = fallbackChain.getAttribute(TypeAttribute.class);
        }
        fallbackSource.setMaxTokenLength(maxTokenLength);
        fallbackSource.setReader(reader);
        fallbackChain.reset();
        usingFallback = true;
    }

    @Override
    public boolean incrementToken() throws IOException {
        if (usingFallback) {
            if (fallbackChain.incrementToken() == false) {
                return false;
            }
            clearAttributes();
            termAtt.copyBuffer(fallbackTerm.buffer(), 0, fallbackTerm.length());
            offsetAtt.setOffset(correctOffset(fallbackOffset.startOffset()), correctOffset(fallbackOffset.endOffset()));
            posIncAtt.setPositionIncrement(fallbackPosInc.getPositionIncrement());
            typeAtt.setType(fallbackType.type());
            return true;
        }

        clearAttributes();
        final char[] buf = buffer;
        final int end = length;
        final int maxLength = maxTokenLength;
        final byte[] classes = CLASSES;
        final byte[] transitions = TRANSITIONS;

        int start = position;
        while (start < end) {
            // Longest match from 'start', inside a window of maxTokenLength chars like the JFlex scanner with a fixed size buffer.
            final int limit = (int) Math.min(end, (long) start + maxLength);
            int state = START;
            int scan = start;
            int lastAccepted = -1;
            boolean letter = false;
            while (scan < limit) {
                state = transitions[(state << CLASS_SHIFT) | classes[buf[scan]]];
                if (state == DEAD) {
                    break;
                }
                scan++;
                if (state >= FIRST_ACCEPTING) {
                    lastAccepted = scan;
                    letter |= state == AFTER_LETTER;
                }
            }

            if (lastAccepted < 0) {
                // No token starts here. If the state machine died before the window ended, every char we passed was an underscore
                // that no token can start from, so we can skip all of them. Otherwise we move one char like the JFlex scanner.
                start = state == DEAD && scan > start ? scan : start + 1;
                continue;
            }

            final int tokenLength = lastAccepted - start;
            final char[] term = termAtt.resizeBuffer(tokenLength);
            final char[] lower = LOWER;
            for (int i = 0; i < tokenLength; i++) {
                term[i] = lower[buf[start + i]];
            }
            termAtt.setLength(tokenLength);
            offsetAtt.setOffset(correctOffset(start), correctOffset(lastAccepted));
            typeAtt.setType(StandardTokenizer.TOKEN_TYPES[letter ? StandardTokenizer.ALPHANUM : StandardTokenizer.NUM]);
            position = lastAccepted;
            return true;
        }
        position = end;
        return false;
    }

    @Override
    public void end() throws IOException {
        super.end();
        if (usingFallback) {
            fallbackChain.end();
            int finalOffset = correctOffset(fallbackOffset.endOffset());
            offsetAtt.setOffset(finalOffset, finalOffset);
            posIncAtt.setPositionIncrement(fallbackPosInc.getPositionIncrement());
        } else {
            int finalOffset = correctOffset(position);
            offsetAtt.setOffset(finalOffset, finalOffset);
        }
    }

    @Override
    public void close() throws IOException {
        try {
            if (usingFallback) {
                usingFallback = false;
                fallbackChain.close();
            }
        } finally {
            super.close();
            if (buffer.length > MAX_RETAINED_BUFFER_CHARS) {
                buffer = new char[INITIAL_BUFFER_CHARS];
            }
        }
    }

    /**
     * Returns the chars of {@code prefix} and then the chars of {@code rest}. Closing it does not close {@code rest} because the
     * tokenizer owns it and closes it.
     */
    private static final class PrefixReader extends Reader {
        private final Reader prefix;
        private final Reader rest;
        private boolean prefixDone;

        PrefixReader(Reader prefix, Reader rest) {
            this.prefix = prefix;
            this.rest = rest;
        }

        @Override
        public int read(char[] cbuf, int off, int len) throws IOException {
            if (prefixDone == false) {
                int read = prefix.read(cbuf, off, len);
                if (read >= 0) {
                    return read;
                }
                prefixDone = true;
            }
            return rest.read(cbuf, off, len);
        }

        @Override
        public void close() throws IOException {
            prefix.close();
        }
    }
}
