/*
 * Licensed to Elastic Search and Shay Banon under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. Elastic Search licenses this
 * file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
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

package org.elasticsearch.common.regex;

import org.elasticsearch.ElasticSearchIllegalArgumentException;
import org.elasticsearch.common.Strings;

import java.util.regex.Pattern;

/**
 * @author kimchy (shay.banon)
 */
public class Regex {

    public static Pattern compile(String regex, String flags) {
        int pFlags = 0;
        if (flags == null) {
            for (String s : Strings.delimitedListToStringArray(flags, "|")) {
                if ("CASE_INSENSITIVE".equalsIgnoreCase(s)) {
                    pFlags |= Pattern.CASE_INSENSITIVE;
                } else if ("MULTILINE".equalsIgnoreCase(s)) {
                    pFlags |= Pattern.MULTILINE;
                } else if ("DOTALL".equalsIgnoreCase(s)) {
                    pFlags |= Pattern.DOTALL;
                } else if ("UNICODE_CASE".equalsIgnoreCase(s)) {
                    pFlags |= Pattern.UNICODE_CASE;
                } else if ("CANON_EQ".equalsIgnoreCase(s)) {
                    pFlags |= Pattern.CANON_EQ;
                } else if ("UNIX_LINES".equalsIgnoreCase(s)) {
                    pFlags |= Pattern.UNIX_LINES;
                } else if ("LITERAL".equalsIgnoreCase(s)) {
                    pFlags |= Pattern.LITERAL;
                } else if ("COMMENTS".equalsIgnoreCase(s)) {
                    pFlags |= Pattern.COMMENTS;
                } else {
                    throw new ElasticSearchIllegalArgumentException("Unknown regex flag [" + s + "] to compile [" + regex + "]");
                }
            }
        }
        return Pattern.compile(regex, pFlags);
    }
}
