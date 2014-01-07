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

import org.apache.lucene.queryparser.XSimpleQueryParser;
import org.elasticsearch.ElasticsearchIllegalArgumentException;
import org.elasticsearch.common.Strings;

import java.util.Locale;

/**
 * Flags for the XSimpleQueryString parser
 */
public enum SimpleQueryStringFlag {
    ALL(-1),
    NONE(0),
    AND(XSimpleQueryParser.AND_OPERATOR),
    NOT(XSimpleQueryParser.NOT_OPERATOR),
    OR(XSimpleQueryParser.OR_OPERATOR),
    PREFIX(XSimpleQueryParser.PREFIX_OPERATOR),
    PRECEDENCE(XSimpleQueryParser.PRECEDENCE_OPERATORS),
    ESCAPE(XSimpleQueryParser.ESCAPE_OPERATOR),
    WHITESPACE(XSimpleQueryParser.WHITESPACE_OPERATOR);

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
                throw new ElasticsearchIllegalArgumentException("Unknown " + SimpleQueryStringParser.NAME + " flag [" + s + "]");
            }
        }
        return magic;
    }
}