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

package org.elasticsearch.common.util.primitives;

import org.elasticsearch.common.Strings;

import java.util.*;

public class Integers {
    /**
     * Tries to parse the given String to an int
     *
     * @param value the String to try to parse to an int
     * @return the parsed value as an int or null if the String can not be parsed to an int
     */
    public static Integer tryParse(String value) {
        if (Strings.isNullOrEmpty(value)) {
            return null;
        } else {
            boolean negative = value.charAt(0) == '-';
            int index = negative ? 1 : 0;
            if (index == value.length()) {
                return null;
            } else {
                int digit = digit(value.charAt(index++));
                if (digit != -1) {
                    // so we can accumulate to Integer.MIN_VALUE
                    int accumulator = -digit;
                    for (int cap = Integer.MIN_VALUE / 10; index < value.length(); accumulator -= digit) {
                        digit = digit(value.charAt(index++));
                        if (digit == -1 || accumulator < cap) {
                            // non-digit or will overflow
                            return null;
                        }
                        accumulator *= 10;
                        if (accumulator < Integer.MIN_VALUE + digit) {
                            // will overflow
                            return null;
                        }
                    }
                    if (negative) {
                        return Integer.valueOf(accumulator);
                    } else if (accumulator == Integer.MIN_VALUE) {
                        // overflow
                        return null;
                    } else {
                        return Integer.valueOf(-accumulator);
                    }
                } else {
                    // non-digit encountered
                    return null;
                }
            }
        }
    }

    private static int digit(char c) {
        return c >= '0' && c <= '9' ? c - '0' : -1;
    }

    public static int[] toArray(Collection<Integer> ints) {
        Objects.requireNonNull(ints);
        return ints.stream().mapToInt(s -> s).toArray();
    }

    public static int checkedCast(long value) {
        int cast = (int)value;
        if ((long)cast != value) {
            throw new IllegalArgumentException(Long.toString(value));
        }
        return cast;
    }
}
