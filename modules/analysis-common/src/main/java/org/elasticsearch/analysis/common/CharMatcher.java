/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.analysis.common;

import java.util.HashSet;
import java.util.Set;

/**
 * A class to match character code points.
 */
public interface CharMatcher {

    class ByUnicodeCategory implements CharMatcher {

        public static CharMatcher of(byte unicodeCategory) {
            return new ByUnicodeCategory(unicodeCategory);
        }

        private final byte unicodeType;

        ByUnicodeCategory(byte unicodeType) {
            this.unicodeType = unicodeType;
        }

        @Override
        public boolean isTokenChar(int c) {
            return Character.getType(c) == unicodeType;
        }
    }

    enum Basic implements CharMatcher {
        LETTER {
            @Override
            public boolean isTokenChar(int c) {
                return Character.isLetter(c);
            }
        },
        DIGIT {
            @Override
            public boolean isTokenChar(int c) {
                return Character.isDigit(c);
            }
        },
        WHITESPACE {
            @Override
            public boolean isTokenChar(int c) {
                return Character.isWhitespace(c);
            }
        },
        PUNCTUATION {
            @Override
            public boolean isTokenChar(int c) {
                switch (Character.getType(c)) {
                    case Character.START_PUNCTUATION:
                    case Character.END_PUNCTUATION:
                    case Character.OTHER_PUNCTUATION:
                    case Character.CONNECTOR_PUNCTUATION:
                    case Character.DASH_PUNCTUATION:
                    case Character.INITIAL_QUOTE_PUNCTUATION:
                    case Character.FINAL_QUOTE_PUNCTUATION:
                        return true;
                    default:
                        return false;
                }
            }
        },
        SYMBOL {
            @Override
            public boolean isTokenChar(int c) {
                switch (Character.getType(c)) {
                    case Character.CURRENCY_SYMBOL:
                    case Character.MATH_SYMBOL:
                    case Character.OTHER_SYMBOL:
                    case Character.MODIFIER_SYMBOL:
                        return true;
                    default:
                        return false;
                }
            }
        }
    }

    final class Builder {
        private final Set<CharMatcher> matchers;

        Builder() {
            matchers = new HashSet<>();
        }

        public Builder or(CharMatcher matcher) {
            matchers.add(matcher);
            return this;
        }

        public CharMatcher build() {
            switch (matchers.size()) {
                case 0:
                    return new CharMatcher() {
                        @Override
                        public boolean isTokenChar(int c) {
                            return false;
                        }
                    };
                case 1:
                    return matchers.iterator().next();
                default:
                    return new CharMatcher() {
                        @Override
                        public boolean isTokenChar(int c) {
                            for (CharMatcher matcher : matchers) {
                                if (matcher.isTokenChar(c)) {
                                    return true;
                                }
                            }
                            return false;
                        }
                    };
            }
        }
    }

    /** Returns true if, and only if, the provided character matches this character class. */
    boolean isTokenChar(int c);
}
