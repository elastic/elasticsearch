/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.ingest.common;

import org.elasticsearch.common.network.InetAddresses;
import org.elasticsearch.ingest.AbstractProcessor;
import org.elasticsearch.ingest.ConfigurationUtils;
import org.elasticsearch.ingest.IngestDocument;
import org.elasticsearch.ingest.Processor;

import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.Map;

import static org.elasticsearch.ingest.ConfigurationUtils.newConfigurationException;

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
                    String strValue = value.toString();
                    if (strValue.startsWith("0x") || strValue.startsWith("-0x")) {
                        return Integer.decode(strValue);
                    }
                    return Integer.parseInt(strValue);
                } catch (NumberFormatException e) {
                    throw new IllegalArgumentException("unable to convert [" + value + "] to integer", e);
                }

            }
        },
        LONG {
            @Override
            public Object convert(Object value) {
                try {
                    String strValue = value.toString();
                    if (strValue.startsWith("0x") || strValue.startsWith("-0x")) {
                        return Long.decode(strValue);
                    }
                    return Long.parseLong(strValue);
                } catch (NumberFormatException e) {
                    throw new IllegalArgumentException("unable to convert [" + value + "] to long", e);
                }
            }
        },
        DOUBLE {
            @Override
            public Object convert(Object value) {
                try {
                    return Double.parseDouble(value.toString());
                } catch (NumberFormatException e) {
                    throw new IllegalArgumentException("unable to convert [" + value + "] to double", e);
                }
            }
        },
        FLOAT {
            @Override
            public Object convert(Object value) {
                try {
                    return Float.parseFloat(value.toString());
                } catch (NumberFormatException e) {
                    throw new IllegalArgumentException("unable to convert [" + value + "] to float", e);
                }
            }
        },
        BOOLEAN {
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
        },
        IP {
            @Override
            public Object convert(Object value) {
                // IllegalArgumentException is thrown if unable to convert
                InetAddresses.forString((String) value);
                return value;
            }
        },
        STRING {
            @Override
            public Object convert(Object value) {
                return value.toString();
            }
        },
        AUTO {
            @Override
            public Object convert(Object value) {
                if ((value instanceof String) == false) {
                    return value;
                }
                try {
                    return BOOLEAN.convert(value);
                } catch (IllegalArgumentException e) {}
                try {
                    return INTEGER.convert(value);
                } catch (IllegalArgumentException e) {}
                try {
                    return LONG.convert(value);
                } catch (IllegalArgumentException e) {}
                try {
                    return FLOAT.convert(value);
                } catch (IllegalArgumentException e) {}
                try {
                    return DOUBLE.convert(value);
                } catch (IllegalArgumentException e) {}
                return value;
            }
        };

        @Override
        public String toString() {
            return name().toLowerCase(Locale.ROOT);
        }

        public abstract Object convert(Object value) throws IllegalArgumentException;

        public static Type fromString(String processorTag, String propertyName, String type) {
            try {
                return Type.valueOf(type.toUpperCase(Locale.ROOT));
            } catch (IllegalArgumentException e) {
                throw newConfigurationException(
                    TYPE,
                    processorTag,
                    propertyName,
                    "type [" + type + "] not supported, cannot convert field."
                );
            }
        }
    }

    public static final String TYPE = "convert";

    private final String field;
    private final String targetField;
    private final Type convertType;
    private final boolean ignoreMissing;
    private final boolean skipFailures;

    ConvertProcessor(
        String tag,
        String description,
        String field,
        String targetField,
        Type convertType,
        boolean ignoreMissing,
        boolean skipFailures
    ) {
        super(tag, description);
        this.field = field;
        this.targetField = targetField;
        this.convertType = convertType;
        this.ignoreMissing = ignoreMissing;
        this.skipFailures = skipFailures;
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

    boolean isIgnoreMissing() {
        return ignoreMissing;
    }

    boolean isSkipFailures() {
        return skipFailures;
    }

    @Override
    public IngestDocument execute(IngestDocument document) {
        Object oldValue = document.getFieldValue(field, Object.class, ignoreMissing);
        Object newValue = null;

        if (oldValue == null && ignoreMissing) {
            return document;
        } else if (oldValue == null && skipFailures == false) {
            throw new IllegalArgumentException("Field [" + field + "] is null, cannot be converted to type [" + convertType + "]");
        }

        if (oldValue instanceof List<?> list) {
            List<Object> newList = new ArrayList<>(list.size());
            for (Object value : list) {
                try {
                    newList.add(convertType.convert(value));
                } catch (IllegalArgumentException e) {
                    if (skipFailures == false) {
                        throw e;
                    }
                }
            }
            newValue = newList;
        } else {
            try {
                newValue = convertType.convert(oldValue);
            } catch (IllegalArgumentException e) {
                if (skipFailures == false) {
                    throw e;
                }
            }
        }
        document.setFieldValue(targetField, newValue);
        return document;
    }

    @Override
    public String getType() {
        return TYPE;
    }

    public static final class Factory implements Processor.Factory {
        @Override
        public ConvertProcessor create(
            Map<String, Processor.Factory> registry,
            String processorTag,
            String description,
            Map<String, Object> config
        ) throws Exception {
            String field = ConfigurationUtils.readStringProperty(TYPE, processorTag, config, "field");
            String typeProperty = ConfigurationUtils.readStringProperty(TYPE, processorTag, config, "type");
            String targetField = ConfigurationUtils.readStringProperty(TYPE, processorTag, config, "target_field", field);
            Type convertType = Type.fromString(processorTag, "type", typeProperty);
            boolean ignoreMissing = ConfigurationUtils.readBooleanProperty(TYPE, processorTag, config, "ignore_missing", false);
            boolean skipFailures = ConfigurationUtils.readBooleanProperty(TYPE, processorTag, config, "skip_failures", false);
            return new ConvertProcessor(processorTag, description, field, targetField, convertType, ignoreMissing, skipFailures);
        }
    }
}
