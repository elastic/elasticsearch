/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.ml.job.categorization;

import java.io.IOException;

/**
 * Java port of the classic ML categorization tokenizer, as implemented in the ML C++ code.
 *
 * In common with the original ML C++ code, there are no configuration options.
 */
public class MlClassicTokenizer extends AbstractMlTokenizer {

    public static final String NAME = "ml_classic";

    MlClassicTokenizer() {}

    /**
     * Basically tokenize into [a-zA-Z0-9]+ strings, but also allowing underscores, dots and dashes in the middle.
     * Then discard tokens that are hex numbers or begin with a digit.
     */
    @Override
    public final boolean incrementToken() throws IOException {
        clearAttributes();
        skippedPositions = 0;

        int start = -1;
        int length = 0;

        boolean haveNonHex = false;
        int curChar;
        while ((curChar = input.read()) >= 0) {
            ++nextOffset;
            if (Character.isLetterOrDigit(curChar) || (length > 0 && (curChar == '_' || curChar == '.' || curChar == '-'))) {
                if (length == 0) {
                    // We're at the first character of a candidate token, so record the offset
                    start = nextOffset - 1;
                }
                termAtt.append((char) curChar);
                ++length;

                // We don't return tokens that are hex numbers, and it's most efficient to keep a running note of this
                haveNonHex = haveNonHex ||
                // Count dots and dashes as numeric
                    (Character.digit(curChar, 16) == -1 && curChar != '.' && curChar != '-');
            } else if (length > 0) {
                // If we get here, we've found a separator character having built up a candidate token

                if (haveNonHex && Character.isDigit(termAtt.charAt(0)) == false) {
                    // The candidate token is valid to return
                    break;
                }

                // The candidate token is not valid to return, i.e. it's hex or begins with a digit, so wipe it and carry on searching
                ++skippedPositions;
                start = -1;
                length = 0;
                termAtt.setEmpty();
            }
        }

        // We need to recheck whether we've got a valid token after the loop because
        // the loop can also be exited on reaching the end of the stream
        if (length == 0) {
            return false;
        }

        if (haveNonHex == false || Character.isDigit(termAtt.charAt(0))) {
            ++skippedPositions;
            return false;
        }

        // Strip dots, dashes and underscores at the end of the token
        char toCheck;
        while ((toCheck = termAtt.charAt(length - 1)) == '_' || toCheck == '.' || toCheck == '-') {
            --length;
        }

        // Characters that may exist in the term attribute beyond its defined length are ignored
        termAtt.setLength(length);
        offsetAtt.setOffset(correctOffset(start), correctOffset(start + length));
        posIncrAtt.setPositionIncrement(skippedPositions + 1);

        return true;
    }
}
