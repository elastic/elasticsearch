/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.ml.inference.nlp.tokenizers;

import java.util.LinkedList;
import java.util.Optional;
import java.util.Queue;

import static java.lang.Character.isDigit;
import static java.lang.Character.isLetter;
import static java.lang.Character.isWhitespace;

/**
 * This is a char sequence tokenizer that satisfies the regex bpe regex for gpt-2
 *      "'s|'t|'re|'ve|'m|'ll|'d| ?\\p{L}+| ?\\p{N}+| ?[^\\s\\p{L}\\p{N}]+|\\s+(?!\\S)|\\s+"
 *
 * In plain english it checks the following:
 *   - An ASCII apostrophe followed by one of a handful of specific letter sequences
 *   - Optional space followed by an unbroken sequence of letters
 *   - Optional space followed by an unbroken sequence of numbers
 *   - Optional space followed by an unbroken sequence of characters that are neither letters, numbers nor whitespace
 *   - Non-empty sequence of whitespace
 *
 * It is by no means thread safe can only can parse one sequence at a time
 */
public class BpeTokenReader {
    private final CharSequence inputChars;
    int count;
    private final Queue<Integer> putBackChar = new LinkedList<>();
    boolean inSpacePrefix = false;
    boolean inAnyLetter = false;
    boolean inAnyNumber = false;
    boolean inSymbol = false;
    boolean inWhiteSpace = false;
    int offsetStart = 0;
    int offsetEnd = 0;

    public BpeTokenReader(CharSequence input) {
        this.inputChars = input;
    }

    public Optional<CharSequenceRef> next() {
        int curIntChar;
        offsetStart = offsetEnd;
        while ((curIntChar = getNextChar()) >= 0) {
            // check for 's|'t|'re|'ve|'m|'ll|'d
            char curChar = (char) curIntChar;
            if (isApostrophe(curChar)) {
                // If we are already matching a selection of notNumbersOrLetters
                if (inSymbol) {
                    offsetEnd++;
                    continue;
                }
                if (inAnythingOtherThanSpace()) {
                    putBackChar.add(curIntChar);
                    return Optional.of(tokenComplete());
                }
                inSymbol = true;
                if (inSpacePrefix) {
                    offsetEnd++;
                    continue;
                }
                offsetEnd++;
                int nextIntChar = getNextChar();
                if (nextIntChar < 0) {
                    return Optional.of(tokenComplete());
                }
                if (nextIntChar == 's' || nextIntChar == 't' || nextIntChar == 'm' || nextIntChar == 'd') {
                    offsetEnd++;
                } else if (nextIntChar == 'r' || nextIntChar == 'v' || nextIntChar == 'l') {
                    int nextNextIntChar = getNextChar();
                    if (nextNextIntChar == 'e' || nextNextIntChar == 'l') {
                        offsetEnd++;
                        offsetEnd++;
                    } else {
                        putBackChar.add(nextIntChar);
                        if (nextNextIntChar >= 0) {
                            putBackChar.add(nextNextIntChar);
                        }
                    }
                } else {
                    putBackChar.add(nextIntChar);
                }
                return Optional.of(tokenComplete());
            }
            // Check for '\p{L}+'
            if (inAnyLetter) {
                if (isLetter(curChar)) {
                    offsetEnd++;
                    continue;
                }
                putBackChar.add(curIntChar);
                return Optional.of(tokenComplete());
            }
            if (isLetter(curChar)) {
                if (inAnythingOtherThanSpace()) {
                    putBackChar.add(curIntChar);
                    return Optional.of(tokenComplete());
                }
                inAnyLetter = true;
                offsetEnd++;
                continue;
            }
            // Check for '\p{N}+'
            if (inAnyNumber) {
                if (isDigit(curChar)) {
                    offsetEnd++;
                    continue;
                }
                putBackChar.add(curIntChar);
                return Optional.of(tokenComplete());
            }
            if (isDigit(curChar)) {
                if (inAnythingOtherThanSpace()) {
                    putBackChar.add(curIntChar);
                    return Optional.of(tokenComplete());
                }
                inAnyNumber = true;
                offsetEnd++;
                continue;
            }
            // Check for '[^\s\p{L}\p{N}]+'
            if (inSymbol) {
                if (isSymbol(curChar)) {
                    offsetEnd++;
                    continue;
                }
                putBackChar.add(curIntChar);
                return Optional.of(tokenComplete());
            }
            if (isSymbol(curChar)) {
                if (inAnythingOtherThanSpace()) {
                    putBackChar.add(curIntChar);
                    return Optional.of(tokenComplete());
                }
                inSymbol = true;
                offsetEnd++;
                continue;
            }
            // checking for \s+(?!\S)|\s+
            if (inWhiteSpace) {
                if (isWhitespace(curChar) && isSpace(curChar) == false) {
                    offsetEnd++;
                    continue;
                }
                if (isSpace(curChar)) {
                    int nextInt = getNextChar();
                    if (nextInt < 0) {
                        offsetEnd++;
                        return Optional.of(tokenComplete());

                    }
                    if (isWhitespace(nextInt)) {
                        offsetEnd++;
                        putBackChar.add(nextInt);
                        continue;
                    }
                    putBackChar.add(curIntChar);
                    putBackChar.add(nextInt);
                    return Optional.of(tokenComplete());
                }
                putBackChar.add(curIntChar);
                return Optional.of(tokenComplete());
            }
            if (isWhitespace(curChar)) {
                if (inAnythingOtherThanSpace()) {
                    putBackChar.add(curIntChar);
                    return Optional.of(tokenComplete());
                }
                if (isSpace(curChar) && inSpacePrefix == false) {
                    offsetEnd++;
                    inSpacePrefix = true;
                    continue;
                }
                if (isSpace(curChar)) {
                    int nextInt = getNextChar();
                    if (nextInt < 0) {
                        offsetEnd++;
                        return Optional.of(tokenComplete());

                    }
                    if (isWhitespace(nextInt)) {
                        inWhiteSpace = true;
                        offsetEnd++;
                        putBackChar.add(nextInt);
                        continue;
                    }
                    putBackChar.add(curIntChar);
                    putBackChar.add(nextInt);
                    return Optional.of(tokenComplete());
                }
                inWhiteSpace = true;
                offsetEnd++;
            }
        }
        if (offsetEnd > offsetStart) {
            return Optional.of(tokenComplete());
        }
        return Optional.empty();
    }

    private CharSequenceRef tokenComplete() {
        inAnyNumber = inAnyLetter = inSymbol = inSpacePrefix = inWhiteSpace = false;
        return new CharSequenceRef(inputChars, offsetStart, offsetEnd - offsetStart);
    }

    private boolean inAnythingOtherThanSpace() {
        return inAnyNumber || inAnyLetter || inSymbol || inWhiteSpace;
    }

    private int getNextChar() {
        if (putBackChar.isEmpty()) {
            if (count >= inputChars.length()) {
                return -1;
            } else {
                return inputChars.charAt(count++);
            }
        }
        return putBackChar.poll();
    }

    private static boolean isSymbol(char c) {
        return isDigit(c) == false && isLetter(c) == false && isWhitespace(c) == false;
    }

    private static boolean isSpace(char c) {
        return c == ' ';
    }

    private static boolean isApostrophe(char c) {
        return c == '\'';
    }

    public record CharSequenceRef(CharSequence wrapped, int offset, int len) implements CharSequence {

        public int getOffset() {
            return offset;
        }

        @Override
        public int length() {
            return len;
        }

        @Override
        public char charAt(int index) {
            return wrapped.charAt(index + offset);
        }

        @Override
        public CharSequence subSequence(int start, int end) {
            return wrapped.subSequence(start + offset, end + offset);
        }

        @Override
        public String toString() {
            return wrapped.subSequence(offset, offset + len).toString();
        }
    }
}
