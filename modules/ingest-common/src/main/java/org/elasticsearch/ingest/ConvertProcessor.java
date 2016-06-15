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

package org.elasticsearch.ingest;

import org.elasticsearch.ingest.core.AbstractProcessor;
import org.elasticsearch.ingest.core.AbstractProcessorFactory;
import org.elasticsearch.ingest.core.IngestDocument;
import org.elasticsearch.ingest.core.ConfigurationUtils;

import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.Map;

import static org.elasticsearch.ingest.core.ConfigurationUtils.newConfigurationException;

/**
 * Processor that converts fields content to a different type. Supported types are: integer, float, boolean and string.
 * Throws exception if the field is not there or the conversion fails.
 */
public final class ConvertProcessor extends AbstractProcessor {

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
        }, AUTO {
            @Override
            public Object convert(Object value) {
                if (!(value instanceof String)) {
                   return value;
                }
                try {
                    return BOOLEAN.convert(value);
                } catch (IllegalArgumentException e) { }
                try {
                    return INTEGER.convert(value);
                } catch (IllegalArgumentException e) {}
                try {
                    return FLOAT.convert(value);
                } catch (IllegalArgumentException e) {}
                return value;
            }
        };

        @Override
        public final String toString() {
            return name().toLowerCase(Locale.ROOT);
        }

        public abstract Object convert(Object value);

        public static Type fromString(String processorTag, String propertyName, String type) {
            try {
                return Type.valueOf(type.toUpperCase(Locale.ROOT));
            } catch(IllegalArgumentException e) {
                throw newConfigurationException(TYPE, processorTag, propertyName, "type [" + type +
                        "] not supported, cannot convert field.");
            }
        }
    }

    public static final String TYPE = "convert";

    private final String field;
    private final String targetField;
    private final Type convertType;

    ConvertProcessor(String tag, String field, String targetField, Type convertType) {
        super(tag);
        this.field = field;
        this.targetField = targetField;
        this.convertType = convertType;
    }

    String getField() {
        return field;
    }

    String getTargetField() {
        return targetField;
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
        document.setFieldValue(targetField, newValue);
    }

    @Override
    public String getType() {
        return TYPE;
    }

    public static final class Factory extends AbstractProcessorFactory<ConvertProcessor> {
        @Override
        public ConvertProcessor doCreate(String processorTag, Map<String, Object> config) throws Exception {
            String field = ConfigurationUtils.readStringProperty(TYPE, processorTag, config, "field");
            String typeProperty = ConfigurationUtils.readStringProperty(TYPE, processorTag, config, "type");
            String targetField = ConfigurationUtils.readStringProperty(TYPE, processorTag, config, "target_field", field);
            Type convertType = Type.fromString(processorTag, "type", typeProperty);
            return new ConvertProcessor(processorTag, field, targetField, convertType);
        }
    }
}
