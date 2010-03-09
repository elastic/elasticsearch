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

package org.elasticsearch.util.json;

import org.codehaus.jackson.*;
import org.codehaus.jackson.map.DeserializationContext;
import org.codehaus.jackson.map.ObjectMapper;
import org.codehaus.jackson.map.SerializerProvider;
import org.codehaus.jackson.map.deser.CustomDeserializerFactory;
import org.codehaus.jackson.map.deser.StdDeserializer;
import org.codehaus.jackson.map.deser.StdDeserializerProvider;
import org.codehaus.jackson.map.ser.CustomSerializerFactory;
import org.codehaus.jackson.map.ser.SerializerBase;
import org.elasticsearch.util.joda.FormatDateTimeFormatter;
import org.elasticsearch.util.joda.Joda;
import org.joda.time.DateTime;

import java.io.IOException;
import java.util.Date;

/**
 * A set of helper methods for Jackson.
 *
 * @author kimchy (Shay Banon)
 */
public final class Jackson {

    private static final JsonFactory defaultJsonFactory;

    private static final ObjectMapper defaultObjectMapper;

    static {
        defaultJsonFactory = newJsonFactory();
        defaultObjectMapper = newObjectMapper();
    }

    public static JsonFactory defaultJsonFactory() {
        return defaultJsonFactory;
    }

    public static ObjectMapper defaultObjectMapper() {
        return defaultObjectMapper;
    }

    public static JsonFactory newJsonFactory() {
        JsonFactory jsonFactory = new JsonFactory();
        jsonFactory.configure(JsonParser.Feature.ALLOW_UNQUOTED_FIELD_NAMES, true);
        jsonFactory.configure(JsonGenerator.Feature.QUOTE_FIELD_NAMES, true);
        return jsonFactory;
    }

    public static ObjectMapper newObjectMapper() {
        ObjectMapper mapper = new ObjectMapper();
        mapper.configure(JsonParser.Feature.ALLOW_UNQUOTED_FIELD_NAMES, true);
        mapper.configure(JsonGenerator.Feature.QUOTE_FIELD_NAMES, true);

        CustomSerializerFactory serializerFactory = new CustomSerializerFactory();
        serializerFactory.addSpecificMapping(Date.class, new DateSerializer());
        serializerFactory.addSpecificMapping(DateTime.class, new DateTimeSerializer());
        mapper.setSerializerFactory(serializerFactory);

        CustomDeserializerFactory deserializerFactory = new CustomDeserializerFactory();
        deserializerFactory.addSpecificMapping(Date.class, new DateDeserializer());
        deserializerFactory.addSpecificMapping(DateTime.class, new DateTimeDeserializer());
        mapper.setDeserializerProvider(new StdDeserializerProvider(deserializerFactory));

        return mapper;
    }

    private Jackson() {

    }

    public static class DateDeserializer extends StdDeserializer<Date> {

        private final FormatDateTimeFormatter formatter = Joda.forPattern("dateTime");

        public DateDeserializer() {
            super(Date.class);
        }

        @Override public Date deserialize(JsonParser jp, DeserializationContext ctxt) throws IOException, JsonProcessingException {
            JsonToken t = jp.getCurrentToken();
            if (t == JsonToken.VALUE_STRING) {
                return new Date(formatter.parser().parseMillis(jp.getText()));
            }
            throw ctxt.mappingException(getValueClass());
        }
    }

    public final static class DateSerializer extends SerializerBase<Date> {

        private final FormatDateTimeFormatter formatter = Joda.forPattern("dateTime");

        @Override public void serialize(Date value, JsonGenerator jgen, SerializerProvider provider) throws IOException, JsonGenerationException {
            jgen.writeString(formatter.parser().print(value.getTime()));
        }

        @Override public JsonNode getSchema(SerializerProvider provider, java.lang.reflect.Type typeHint) {
            return createSchemaNode("string", true);
        }
    }

    public static class DateTimeDeserializer extends StdDeserializer<DateTime> {

        private final FormatDateTimeFormatter formatter = Joda.forPattern("dateTime");

        public DateTimeDeserializer() {
            super(DateTime.class);
        }

        @Override public DateTime deserialize(JsonParser jp, DeserializationContext ctxt) throws IOException, JsonProcessingException {
            JsonToken t = jp.getCurrentToken();
            if (t == JsonToken.VALUE_STRING) {
                return formatter.parser().parseDateTime(jp.getText());
            }
            throw ctxt.mappingException(getValueClass());
        }
    }


    public final static class DateTimeSerializer extends SerializerBase<DateTime> {

        private final FormatDateTimeFormatter formatter = Joda.forPattern("dateTime");

        @Override public void serialize(DateTime value, JsonGenerator jgen, SerializerProvider provider) throws IOException, JsonGenerationException {
            jgen.writeString(formatter.printer().print(value));
        }

        @Override public JsonNode getSchema(SerializerProvider provider, java.lang.reflect.Type typeHint) {
            return createSchemaNode("string", true);
        }
    }
}
