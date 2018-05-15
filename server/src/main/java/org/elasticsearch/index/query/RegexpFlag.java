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
package org.elasticsearch.index.query;

import org.apache.lucene.util.automaton.RegExp;
import org.elasticsearch.common.Strings;

import java.util.Locale;

/**
 * Regular expression syntax flags. Each flag represents optional syntax support in the regular expression:
 * <ul>
 *     <li>{@code INTERSECTION} - Support for intersection notation: {@code &lt;expression&gt; &amp; &lt;expression&gt;}</li>
 *     <li>{@code COMPLEMENT} - Support for complement notation: {@code &lt;expression&gt; &amp; &lt;expression&gt;}</li>
 *     <li>{@code EMPTY} - Support for the empty language symbol: {@code #}</li>
 *     <li>{@code ANYSTRING} - Support for the any string symbol: {@code @}</li>
 *     <li>{@code INTERVAL} - Support for numerical interval notation: {@code &lt;n-m&gt;}</li>
 *     <li>{@code NONE} - Disable support for all syntax options</li>
 *     <li>{@code ALL} - Enables support for all syntax options</li>
 * </ul>
 *
 * @see RegexpQueryBuilder#flags(RegexpFlag...)
 * @see RegexpQueryBuilder#flags(RegexpFlag...)
 */
public enum RegexpFlag {

    /**
     * Enables intersection of the form: {@code &lt;expression&gt; &amp; &lt;expression&gt;}
     */
    INTERSECTION(RegExp.INTERSECTION),

    /**
     * Enables complement expression of the form: {@code ~&lt;expression&gt;}
     */
    COMPLEMENT(RegExp.COMPLEMENT),

    /**
     * Enables empty language expression: {@code #}
     */
    EMPTY(RegExp.EMPTY),

    /**
     * Enables any string expression: {@code @}
     */
    ANYSTRING(RegExp.ANYSTRING),

    /**
     * Enables numerical interval expression: {@code &lt;n-m&gt;}
     */
    INTERVAL(RegExp.INTERVAL),

    /**
     * Disables all available option flags
     */
    NONE(RegExp.NONE),

    /**
     * Enables all available option flags
     */
    ALL(RegExp.ALL);


    final int value;

    RegexpFlag(int value) {
        this.value = value;
    }

    public int value() {
        return value;
    }

    /**
     * Resolves the combined OR'ed value for the given list of regular expression flags. The given flags must follow the
     * following syntax:
     * <p>
     * {@code flag_name}(|{@code flag_name})*
     * <p>
     * Where {@code flag_name} is one of the following:
     * <ul>
     *     <li>INTERSECTION</li>
     *     <li>COMPLEMENT</li>
     *     <li>EMPTY</li>
     *     <li>ANYSTRING</li>
     *     <li>INTERVAL</li>
     *     <li>NONE</li>
     *     <li>ALL</li>
     * </ul>
     * <p>
     * Example: {@code INTERSECTION|COMPLEMENT|EMPTY}
     *
     * @param flags A string representing a list of regular expression flags
     * @return The combined OR'ed value for all the flags
     */
    public static int resolveValue(String flags) {
        if (flags == null || flags.isEmpty()) {
            return RegExp.ALL;
        }
        int magic = RegExp.NONE;
        for (String s : Strings.delimitedListToStringArray(flags, "|")) {
            if (s.isEmpty()) {
                continue;
            }
            try {
                RegexpFlag flag = RegexpFlag.valueOf(s.toUpperCase(Locale.ROOT));
                if (flag == RegexpFlag.NONE) {
                    continue;
                }
                if (flag == RegexpFlag.ALL) {
                    return flag.value();
                }
                magic |= flag.value();
            } catch (IllegalArgumentException iae) {
                throw new IllegalArgumentException("Unknown regexp flag [" + s + "]");
            }
        }
        return magic;
    }
}
