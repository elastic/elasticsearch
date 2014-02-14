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

import java.util.Locale;

import org.apache.lucene.util.automaton.RegExp;
import org.elasticsearch.ElasticsearchIllegalArgumentException;
import org.elasticsearch.common.Strings;

/**
 * Regular expression syntax flags. Each flag represents optional syntax support in the regular expression:
 * <ul>
 *     <li><tt>INTERSECTION</tt> - Support for intersection notation: <tt>&lt;expression&gt; &amp; &lt;expression&gt;</tt></li>
 *     <li><tt>COMPLEMENT</tt> - Support for complement notation: <tt>&lt;expression&gt; &amp; &lt;expression&gt;</tt></li>
 *     <li><tt>EMPTY</tt> - Support for the empty language symbol: <tt>#</tt></li>
 *     <li><tt>ANYSTRING</tt> - Support for the any string symbol: <tt>@</tt></li>
 *     <li><tt>INTERVAL</tt> - Support for numerical interval notation: <tt>&lt;n-m&gt;</tt></li>
 *     <li><tt>NONE</tt> - Disable support for all syntax options</li>
 *     <li><tt>ALL</tt> - Enables support for all syntax options</li>
 * </ul>
 *
 * @see RegexpQueryBuilder#flags(RegexpFlag...)
 * @see RegexpFilterBuilder#flags(RegexpFlag...)
 */
public enum RegexpFlag {

    /**
     * Enables intersection of the form: <tt>&lt;expression&gt; &amp; &lt;expression&gt;</tt>
     */
    INTERSECTION(RegExp.INTERSECTION),

    /**
     * Enables complement expression of the form: <tt>~&lt;expression&gt;</tt>
     */
    COMPLEMENT(RegExp.COMPLEMENT),

    /**
     * Enables empty language expression: <tt>#</tt>
     */
    EMPTY(RegExp.EMPTY),

    /**
     * Enables any string expression: <tt>@</tt>
     */
    ANYSTRING(RegExp.ANYSTRING),

    /**
     * Enables numerical interval expression: <tt>&lt;n-m&gt;</tt>
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

    private RegexpFlag(int value) {
        this.value = value;
    }

    public int value() {
        return value;
    }

    /**
     * Resolves the combined OR'ed value for the given list of regular expression flags. The given flags must follow the
     * following syntax:
     * <p/>
     * <tt>flag_name</tt>(|<tt>flag_name</tt>)*
     * <p/>
     * Where <tt>flag_name</tt> is one of the following:
     * <ul>
     *     <li>INTERSECTION</li>
     *     <li>COMPLEMENT</li>
     *     <li>EMPTY</li>
     *     <li>ANYSTRING</li>
     *     <li>INTERVAL</li>
     *     <li>NONE</li>
     *     <li>ALL</li>
     * </ul>
     * <p/>
     * Example: <tt>INTERSECTION|COMPLEMENT|EMPTY</tt>
     *
     * @param flags A string representing a list of regualr expression flags
     * @return The combined OR'ed value for all the flags
     */
    static int resolveValue(String flags) {
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
                throw new ElasticsearchIllegalArgumentException("Unknown regexp flag [" + s + "]");
            }
        }
        return magic;
    }
}
