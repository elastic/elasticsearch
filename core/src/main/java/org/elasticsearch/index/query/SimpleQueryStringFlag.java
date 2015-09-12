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

import org.elasticsearch.common.Strings;

import java.util.Locale;

/**
 * Flags for the XSimpleQueryString parser
 */
public enum SimpleQueryStringFlag {
    ALL(-1),
    NONE(0),
    AND(SimpleQueryParser.AND_OPERATOR),
    NOT(SimpleQueryParser.NOT_OPERATOR),
    OR(SimpleQueryParser.OR_OPERATOR),
    PREFIX(SimpleQueryParser.PREFIX_OPERATOR),
    PHRASE(SimpleQueryParser.PHRASE_OPERATOR),
    PRECEDENCE(SimpleQueryParser.PRECEDENCE_OPERATORS),
    ESCAPE(SimpleQueryParser.ESCAPE_OPERATOR),
    WHITESPACE(SimpleQueryParser.WHITESPACE_OPERATOR),
    FUZZY(SimpleQueryParser.FUZZY_OPERATOR),
    // NEAR and SLOP are synonymous, since "slop" is a more familiar term than "near"
    NEAR(SimpleQueryParser.NEAR_OPERATOR),
    SLOP(SimpleQueryParser.NEAR_OPERATOR);

    final int value;

    private SimpleQueryStringFlag(int value) {
        this.value = value;
    }

    public int value() {
        return value;
    }

    static int resolveFlags(String flags) {
        if (!Strings.hasLength(flags)) {
            return ALL.value();
        }
        int magic = NONE.value();
        for (String s : Strings.delimitedListToStringArray(flags, "|")) {
            if (s.isEmpty()) {
                continue;
            }
            try {
                SimpleQueryStringFlag flag = SimpleQueryStringFlag.valueOf(s.toUpperCase(Locale.ROOT));
                switch (flag) {
                    case NONE:
                        return 0;
                    case ALL:
                        return -1;
                    default:
                        magic |= flag.value();
                }
            } catch (IllegalArgumentException iae) {
                throw new IllegalArgumentException("Unknown " + SimpleQueryStringParser.NAME + " flag [" + s + "]");
            }
        }
        return magic;
    }
}
