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

import com.fasterxml.jackson.core.io.JsonStringEncoder;
import org.apache.logging.log4j.message.ParameterizedMessage;
import org.elasticsearch.common.SuppressLoggerChecks;

import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * A base class for custom log4j logger messages. Carries additional fields which will populate JSON fields in logs.
 */
public class ESLogMessage extends ParameterizedMessage {
    private static final JsonStringEncoder JSON_STRING_ENCODER = JsonStringEncoder.getInstance();
    private final Map<String, Object> fields;

    /**
     * This is an abstract class, so this is safe. The check is done on DeprecationMessage.
     * Other subclasses are not allowing varargs
     */
    @SuppressLoggerChecks(reason = "Safe as this is abstract class")
    public ESLogMessage(Map<String, Object> fields, String messagePattern, Object... args) {
        super(messagePattern, args);
        this.fields = fields;
    }

    public ESLogMessage with(String key, Object value) {
        fields.put(key, value);
        return this;
    }
    public static String escapeJson(String text) {
        byte[] sourceEscaped = JSON_STRING_ENCODER.quoteAsUTF8(text);
        return new String(sourceEscaped, Charset.defaultCharset());
    }

    public Map<String,Object> getFields(){
        return fields;
    }

    public String getValueFor(String key) {
        Object value = fields.get(key);
        return value!=null ? value.toString() : null;
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

    public static ESLogMessageBuilder message(String messagePattern){
        return new ESLogMessageBuilder().message(messagePattern);
    }

    public static class ESLogMessageBuilder {
        private String parametrizedMessage;
        private List<Object> parameters = new ArrayList<>();
        private  Map<String, Object> fields = new LinkedHashMap<>();

        public ESLogMessageBuilder message(String parametrizedMessage){
            this.parametrizedMessage = parametrizedMessage;
            return this;
        }

        public ESLogMessageBuilder param(Object object){
            parameters.add(object);
            return this;
        }

        public ESLogMessageBuilder paramField(String key, Object value){
            parameters.add(value);
            fields.put(key,value);
            return this;
        }

        public ESLogMessageBuilder field(String key, Object value){
            fields.put(key,value);
            return this;
        }

        public ESLogMessage build(){
            return new ESLogMessage(fields, parametrizedMessage, parameters.toArray()) ;
        }

    }
}
