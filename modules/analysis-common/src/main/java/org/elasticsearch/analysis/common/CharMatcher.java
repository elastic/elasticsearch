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
                return switch (Character.getType(c)) {
        // tag::noformat
                    case Character.START_PUNCTUATION, Character.END_PUNCTUATION, Character.OTHER_PUNCTUATION,
                         Character.CONNECTOR_PUNCTUATION, Character.DASH_PUNCTUATION, Character.INITIAL_QUOTE_PUNCTUATION,
                         Character.FINAL_QUOTE_PUNCTUATION -> true;
        // end::noformat
                    default -> false;
                };
            }
        },
        SYMBOL {
            @Override
            public boolean isTokenChar(int c) {
                return switch (Character.getType(c)) {
                    case Character.CURRENCY_SYMBOL, Character.MATH_SYMBOL, Character.OTHER_SYMBOL, Character.MODIFIER_SYMBOL -> true;
                    default -> false;
                };
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
            return switch (matchers.size()) {
                case 0 -> new CharMatcher() {
                    @Override
                    public boolean isTokenChar(int c) {
                        return false;
                    }
                };
                case 1 -> matchers.iterator().next();
                default -> new CharMatcher() {
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
            };
        }
    }

    /** Returns true if, and only if, the provided character matches this character class. */
    boolean isTokenChar(int c);
}
