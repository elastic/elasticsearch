/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.analysis.common;

import org.apache.lucene.analysis.Tokenizer;
import org.apache.lucene.analysis.util.CharTokenizer;
import org.apache.lucene.util.AttributeFactory;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.env.Environment;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.index.analysis.AbstractTokenizerFactory;

import java.util.HashSet;
import java.util.Set;

public class CharGroupTokenizerFactory extends AbstractTokenizerFactory {

    static final String MAX_TOKEN_LENGTH = "max_token_length";

    private final Set<Integer> tokenizeOnChars = new HashSet<>();
    private final Integer maxTokenLength;
    private boolean tokenizeOnSpace = false;
    private boolean tokenizeOnLetter = false;
    private boolean tokenizeOnDigit = false;
    private boolean tokenizeOnPunctuation = false;
    private boolean tokenizeOnSymbol = false;

    public CharGroupTokenizerFactory(IndexSettings indexSettings, Environment environment, String name, Settings settings) {
        super(name);

        maxTokenLength = settings.getAsInt(MAX_TOKEN_LENGTH, CharTokenizer.DEFAULT_MAX_WORD_LEN);

        for (final String c : settings.getAsList("tokenize_on_chars")) {
            if (c == null || c.length() == 0) {
                throw new RuntimeException("[tokenize_on_chars] cannot contain empty characters");
            }

            if (c.length() == 1) {
                tokenizeOnChars.add((int) c.charAt(0));
            } else if (c.charAt(0) == '\\') {
                tokenizeOnChars.add((int) parseEscapedChar(c));
            } else {
                switch (c) {
                    case "letter" -> tokenizeOnLetter = true;
                    case "digit" -> tokenizeOnDigit = true;
                    case "whitespace" -> tokenizeOnSpace = true;
                    case "punctuation" -> tokenizeOnPunctuation = true;
                    case "symbol" -> tokenizeOnSymbol = true;
                    default -> throw new RuntimeException("Invalid escaped char in [" + c + "]");
                }
            }
        }
    }

    private static char parseEscapedChar(final String s) {
        int len = s.length();
        char c = s.charAt(0);
        if (c == '\\') {
            if (1 >= len) throw new RuntimeException("Invalid escaped char in [" + s + "]");
            c = s.charAt(1);
            switch (c) {
                case '\\':
                    return '\\';
                case 'n':
                    return '\n';
                case 't':
                    return '\t';
                case 'r':
                    return '\r';
                case 'b':
                    return '\b';
                case 'f':
                    return '\f';
                case 'u':
                    if (len > 6) {
                        throw new RuntimeException("Invalid escaped char in [" + s + "]");
                    }
                    return (char) Integer.parseInt(s.substring(2), 16);
                default:
                    throw new RuntimeException("Invalid escaped char " + c + " in [" + s + "]");
            }
        } else {
            throw new RuntimeException("Invalid escaped char [" + s + "]");
        }
    }

    @Override
    public Tokenizer create() {
        return new CharTokenizer(AttributeFactory.DEFAULT_ATTRIBUTE_FACTORY, maxTokenLength) {
            @Override
            protected boolean isTokenChar(int c) {
                if (tokenizeOnSpace && Character.isWhitespace(c)) {
                    return false;
                }
                if (tokenizeOnLetter && Character.isLetter(c)) {
                    return false;
                }
                if (tokenizeOnDigit && Character.isDigit(c)) {
                    return false;
                }
                if (tokenizeOnPunctuation && CharMatcher.Basic.PUNCTUATION.isTokenChar(c)) {
                    return false;
                }
                if (tokenizeOnSymbol && CharMatcher.Basic.SYMBOL.isTokenChar(c)) {
                    return false;
                }
                return tokenizeOnChars.contains(c) == false;
            }
        };
    }
}
