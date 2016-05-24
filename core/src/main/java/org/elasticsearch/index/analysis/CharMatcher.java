/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.elasticsearch.index.analysis;

import java.util.HashSet;
import java.util.Set;

/**
 * A class to match character code points.
 */
public interface CharMatcher {

    public static class ByUnicodeCategory implements CharMatcher {

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

    public enum Basic implements CharMatcher {
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

    public final class Builder {
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
    public boolean isTokenChar(int c);
}
