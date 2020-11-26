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
package org.elasticsearch.common.logging;

import org.apache.logging.log4j.message.MapMessage;
import org.apache.logging.log4j.message.ParameterizedMessage;
import org.apache.logging.log4j.util.Chars;
import org.apache.logging.log4j.util.StringBuilders;

import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * A base class for custom log4j logger messages. Carries additional fields which will populate JSON fields in logs.
 */
public class ESLogMessage extends MapMessage<ESLogMessage, Object> {
    private final List<Object> arguments = new ArrayList<>();
    private String messagePattern;

    public ESLogMessage(String messagePattern, Object... args) {
        super(new LinkedHashMap<>());
        Collections.addAll(this.arguments, args);
        this.messagePattern = messagePattern;

        Object message = new Object() {
            @Override
            public String toString() {
                return ParameterizedMessage.format(messagePattern, arguments.toArray());
            }
        };
        with("message", message);
    }

    public ESLogMessage() {
        super(new LinkedHashMap<>());
    }

    public ESLogMessage argAndField(String key, Object value) {
        this.arguments.add(value);
        super.with(key, value);
        return this;
    }

    public ESLogMessage field(String key, Object value) {
        super.with(key, value);
        return this;
    }

    public ESLogMessage withFields(Map<String, Object> prepareMap) {
        prepareMap.forEach(this::field);
        return this;
    }

    /**
     * This method is used in order to support ESJsonLayout which replaces %CustomMapFields from a pattern with JSON fields
     * It is a modified version of {@link MapMessage#asJson(StringBuilder)} where the curly brackets are not added
     * @param sb a string builder where JSON fields will be attached
     */
    protected void addJsonNoBrackets(StringBuilder sb) {
        for (int i = 0; i < getIndexedReadOnlyStringMap().size(); i++) {
            if (i > 0) {
                sb.append(", ");
            }
            sb.append(Chars.DQUOTE);
            int start = sb.length();
            sb.append(getIndexedReadOnlyStringMap().getKeyAt(i));
            StringBuilders.escapeJson(sb, start);
            sb.append(Chars.DQUOTE).append(':').append(Chars.DQUOTE);
            start = sb.length();
            sb.append((Object) getIndexedReadOnlyStringMap().getValueAt(i));
            StringBuilders.escapeJson(sb, start);
            sb.append(Chars.DQUOTE);
        }
    }

    public static String inQuotes(String s) {
        if(s == null)
            return inQuotes("");
        return "\"" + s + "\"";
    }

    public static String inQuotes(Object s) {
        if(s == null)
            return inQuotes("");
        return inQuotes(s.toString());
    }

    public static String asJsonArray(Stream<String> stream) {
        return "[" + stream
            .map(ESLogMessage::inQuotes)
            .collect(Collectors.joining(", ")) + "]";
    }

    public Object[] getArguments() {
        return arguments.toArray();
    }

    public String getMessagePattern() {
        return messagePattern;
    }
}
