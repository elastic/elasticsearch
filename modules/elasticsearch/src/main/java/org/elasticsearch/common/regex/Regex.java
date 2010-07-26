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
        int pFlags = flags == null ? 0 : flagsFromString(flags);
        return Pattern.compile(regex, pFlags);
    }

    public static int flagsFromString(String flags) {
        int pFlags = 0;
        for (String s : Strings.delimitedListToStringArray(flags, "|")) {
            if (s.isEmpty()) {
                continue;
            }
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
                throw new ElasticSearchIllegalArgumentException("Unknown regex flag [" + s + "]");
            }
        }
        return pFlags;
    }

    public static String flagsToString(int flags) {
        StringBuilder sb = new StringBuilder();
        if ((flags & Pattern.CASE_INSENSITIVE) != 0) {
            sb.append("CASE_INSENSITIVE|");
        }
        if ((flags & Pattern.MULTILINE) != 0) {
            sb.append("MULTILINE|");
        }
        if ((flags & Pattern.DOTALL) != 0) {
            sb.append("DOTALL|");
        }
        if ((flags & Pattern.UNICODE_CASE) != 0) {
            sb.append("UNICODE_CASE|");
        }
        if ((flags & Pattern.CANON_EQ) != 0) {
            sb.append("CANON_EQ|");
        }
        if ((flags & Pattern.UNIX_LINES) != 0) {
            sb.append("UNIX_LINES|");
        }
        if ((flags & Pattern.LITERAL) != 0) {
            sb.append("LITERAL|");
        }
        if ((flags & Pattern.COMMENTS) != 0) {
            sb.append("COMMENTS|");
        }
        return sb.toString();
    }
}
