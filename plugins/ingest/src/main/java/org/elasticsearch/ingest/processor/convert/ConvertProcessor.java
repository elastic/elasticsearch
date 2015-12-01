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

package org.elasticsearch.ingest.processor.convert;

import org.elasticsearch.ingest.IngestDocument;
import org.elasticsearch.ingest.processor.ConfigurationUtils;
import org.elasticsearch.ingest.processor.Processor;

import java.util.*;

/**
 * Processor that converts fields content to a different type. Supported types are: integer, float, boolean and string.
 * Throws exception if the field is not there or the conversion fails.
 */
public class ConvertProcessor implements Processor {

    enum Type {
        INTEGER {
            @Override
            public Object convert(Object value) {
                try {
                    return Integer.parseInt(value.toString());
                } catch(NumberFormatException e) {
                    throw new IllegalArgumentException("unable to convert [" + value + "] to integer", e);
                }

            }
        }, FLOAT {
            @Override
            public Object convert(Object value) {
                try {
                    return Float.parseFloat(value.toString());
                } catch(NumberFormatException e) {
                    throw new IllegalArgumentException("unable to convert [" + value + "] to float", e);
                }
            }
        }, BOOLEAN {
            @Override
            public Object convert(Object value) {
                if (value.toString().equalsIgnoreCase("true")) {
                    return true;
                } else if (value.toString().equalsIgnoreCase("false")) {
                    return false;
                } else {
                    throw new IllegalArgumentException("[" + value + "] is not a boolean value, cannot convert to boolean");
                }
            }
        }, STRING {
            @Override
            public Object convert(Object value) {
                return value.toString();
            }
        };

        @Override
        public final String toString() {
            return name().toLowerCase(Locale.ROOT);
        }

        public abstract Object convert(Object value);

        public static Type fromString(String type) {
            try {
                return Type.valueOf(type.toUpperCase(Locale.ROOT));
            } catch(IllegalArgumentException e) {
                throw new IllegalArgumentException("type [" + type + "] not supported, cannot convert field.", e);
            }
        }
    }

    public static final String TYPE = "convert";

    private final String field;
    private final Type convertType;

    ConvertProcessor(String field, Type convertType) {
        this.field = field;
        this.convertType = convertType;
    }

    String getField() {
        return field;
    }

    Type getConvertType() {
        return convertType;
    }

    @Override
    public void execute(IngestDocument document) {
        Object oldValue = document.getFieldValue(field, Object.class);
        Object newValue;
        if (oldValue == null) {
            throw new IllegalArgumentException("Field [" + field + "] is null, cannot be converted to type [" + convertType + "]");
        }

        if (oldValue instanceof List) {
            List<?> list = (List<?>) oldValue;
            List<Object> newList = new ArrayList<>();
            for (Object value : list) {
                newList.add(convertType.convert(value));
            }
            newValue = newList;
        } else {
            newValue = convertType.convert(oldValue);
        }
        document.setFieldValue(field, newValue);
    }

    @Override
    public String getType() {
        return TYPE;
    }

    public static class Factory implements Processor.Factory<ConvertProcessor> {
        @Override
        public ConvertProcessor create(Map<String, Object> config) throws Exception {
            String field = ConfigurationUtils.readStringProperty(config, "field");
            Type convertType = Type.fromString(ConfigurationUtils.readStringProperty(config, "type"));
            return new ConvertProcessor(field, convertType);
        }
    }
}
