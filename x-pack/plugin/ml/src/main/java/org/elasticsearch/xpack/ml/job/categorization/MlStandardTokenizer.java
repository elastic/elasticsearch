/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.ml.job.categorization;

import java.io.IOException;
import java.io.Reader;

/**
 * The new standard ML categorization tokenizer. This differs from the "classic"
 * ML tokenizer in that it treats URLs and paths as a single token.
 *
 * In common with the original ML C++ code, there are no configuration options.
 */
public class MlStandardTokenizer extends AbstractMlTokenizer {

    public static String NAME = "ml_standard";

    private int putBackChar = -1;

    MlStandardTokenizer() {}

    /**
     * Basically tokenize into [a-zA-Z0-9]+ strings, but also allowing forward slashes, and underscores, dots and dashes in the middle.
     * Additionally, one colon is allowed, providing only characters come before it and it's followed by a slash, and forward slashes
     * are allowed if we've previously seen a colon.  These rules are designed to keep URLs plus Unix and Windows paths as single tokens.
     * Windows paths may use a drive letter, e.g. C:\whatever, or may be UNC, e.g. \\myserver\folder.
     * We discard tokens that are hex numbers or begin with a digit.
     */
    @Override
    public final boolean incrementToken() throws IOException {
        clearAttributes();
        skippedPositions = 0;

        int start = -1;
        int length = 0;

        boolean haveNonHex = false;
        int lettersBeforeColon = 0;
        boolean haveColon = false;
        int firstBackslashPos = -1;
        int firstForwardSlashPos = -1;
        int slashCount = 0;
        int curChar;
        while ((curChar = getNextChar()) >= 0) {
            ++nextOffset;
            if (Character.isLetterOrDigit(curChar)
                || (length > 0
                    && (curChar == '_'
                        || curChar == '.'
                        || curChar == '-'
                        || curChar == '@'
                        || (curChar == ':' && lettersBeforeColon == length)))
                || curChar == '/'
                || (curChar == '\\' && (length == 0 || (haveColon && lettersBeforeColon == 1) || firstBackslashPos == 0))) {
                if (length == 0) {
                    // We're at the first character of a candidate token, so record the offset
                    start = nextOffset - 1;
                }
                termAtt.append((char) curChar);
                ++length;

                // Tracking related to colons and slashes
                if (curChar == ':') {
                    haveColon = true;
                } else if (curChar == '/') {
                    ++slashCount;
                    if (firstForwardSlashPos == -1) {
                        firstForwardSlashPos = length - 1;
                    }
                } else if (curChar == '\\') {
                    ++slashCount;
                    if (firstBackslashPos == -1) {
                        firstBackslashPos = length - 1;
                    }
                } else {
                    if (haveColon) {
                        if (firstBackslashPos != lettersBeforeColon + 1 && firstForwardSlashPos != lettersBeforeColon + 1) {
                            // If our token contains a colon but not followed by a slash, drop the colon and everything after it
                            assert length - lettersBeforeColon == 2;
                            length -= 2;
                            putBackChar = curChar;
                            --nextOffset;
                            break;
                        }
                    } else if (Character.isLetter(curChar)) {
                        ++lettersBeforeColon;
                    }
                }

                // We don't return tokens that are hex numbers, and it's most efficient to keep a running note of this
                haveNonHex = haveNonHex ||
                // Count dots, dashes, at symbols and colons as numeric
                    (Character.digit(curChar, 16) == -1 && curChar != '.' && curChar != '-' && curChar != '@' && curChar != ':');
            } else if (length > 0) {
                // If we get here, we've found a separator character having built up a candidate token

                if (haveNonHex && Character.isDigit(termAtt.charAt(0)) == false && length > slashCount) {
                    // The candidate token is valid to return
                    break;
                }

                // The candidate token is not valid to return, i.e. it's hex, begins with a digit or all slashes,
                // so wipe it and carry on searching
                ++skippedPositions;
                start = -1;
                length = 0;
                termAtt.setEmpty();

                haveNonHex = false;
                lettersBeforeColon = 0;
                haveColon = false;
                firstBackslashPos = -1;
                firstForwardSlashPos = -1;
                slashCount = 0;
            }
        }

        // We need to recheck whether we've got a valid token after the loop because
        // the loop can also be exited on reaching the end of the stream
        if (length == 0) {
            return false;
        }

        if (haveNonHex == false || Character.isDigit(termAtt.charAt(0)) || length == slashCount) {
            ++skippedPositions;
            return false;
        }

        // Strip dots, dashes, underscores, at symbols and colons at the end of the token
        char toCheck;
        while ((toCheck = termAtt.charAt(length - 1)) == '_' || toCheck == '.' || toCheck == '-' || toCheck == '@' || toCheck == ':') {
            --length;
        }

        // Characters that may exist in the term attribute beyond its defined length are ignored
        termAtt.setLength(length);
        offsetAtt.setOffset(correctOffset(start), correctOffset(start + length));
        posIncrAtt.setPositionIncrement(skippedPositions + 1);

        return true;
    }

    /**
     * Augments a {@link Reader} with a single character putback facility.
     * This means that the putback facility is available regardless of
     * whether {@link Reader#mark} is implemented.
     */
    private int getNextChar() throws IOException {
        int nextChar;
        if (putBackChar >= 0) {
            nextChar = putBackChar;
            putBackChar = -1;
        } else {
            nextChar = input.read();
        }
        return nextChar;
    }
}
