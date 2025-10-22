/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.ingest.common;

import org.elasticsearch.cluster.metadata.ProjectId;
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
                    String strValue = (String) STRING.convert(value);
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
                    String strValue = (String) STRING.convert(value);
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
                if (isExactIntegerDouble(value)) {
                    return String.valueOf(((Double) value).longValue());
                }
                if (isExactIntegerFloat(value)) {
                    return String.valueOf(((Float) value).longValue());
                }
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
                } catch (IllegalArgumentException ignored) {}
                try {
                    return INTEGER.convert(value);
                } catch (IllegalArgumentException ignored) {}
                try {
                    return LONG.convert(value);
                } catch (IllegalArgumentException ignored) {}
                try {
                    return FLOAT.convert(value);
                } catch (IllegalArgumentException ignored) {}
                try {
                    return DOUBLE.convert(value);
                } catch (IllegalArgumentException ignored) {}
                return value;
            }
        };

        @Override
        public String toString() {
            return name().toLowerCase(Locale.ROOT);
        }

        public abstract Object convert(Object value);

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

        private static boolean isExactIntegerFloat(Object value) {
            final float ABS_MAX_EXACT_FLOAT = 0x1p24f - 1;
            if (value instanceof Float == false) {
                return false;
            }
            float v = (Float) value;
            return v == (long) v && -ABS_MAX_EXACT_FLOAT <= v && v <= ABS_MAX_EXACT_FLOAT;
        }

        private static boolean isExactIntegerDouble(Object value) {
            final double ABS_MAX_EXACT_DOUBLE = 0x1p53 - 1;
            if (value instanceof Double == false) {
                return false;
            }
            double v = (Double) value;
            return v == (long) v && -ABS_MAX_EXACT_DOUBLE <= v && v <= ABS_MAX_EXACT_DOUBLE;
        }
    }

    public static final String TYPE = "convert";

    private final String field;
    private final String targetField;
    private final Type convertType;
    private final boolean ignoreMissing;

    ConvertProcessor(String tag, String description, String field, String targetField, Type convertType, boolean ignoreMissing) {
        super(tag, description);
        this.field = field;
        this.targetField = targetField;
        this.convertType = convertType;
        this.ignoreMissing = ignoreMissing;
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

    @Override
    public IngestDocument execute(IngestDocument document) {
        Object oldValue = document.getFieldValue(field, Object.class, ignoreMissing);
        Object newValue;

        if (oldValue == null && ignoreMissing) {
            return document;
        } else if (oldValue == null) {
            throw new IllegalArgumentException("Field [" + field + "] is null, cannot be converted to type [" + convertType + "]");
        }

        if (oldValue instanceof List<?> list) {
            List<Object> newList = new ArrayList<>(list.size());
            for (Object value : list) {
                newList.add(convertType.convert(value));
            }
            newValue = newList;
        } else {
            newValue = convertType.convert(oldValue);
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
            Map<String, Object> config,
            ProjectId projectId
        ) throws Exception {
            String field = ConfigurationUtils.readStringProperty(TYPE, processorTag, config, "field");
            String typeProperty = ConfigurationUtils.readStringProperty(TYPE, processorTag, config, "type");
            String targetField = ConfigurationUtils.readStringProperty(TYPE, processorTag, config, "target_field", field);
            Type convertType = Type.fromString(processorTag, "type", typeProperty);
            boolean ignoreMissing = ConfigurationUtils.readBooleanProperty(TYPE, processorTag, config, "ignore_missing", false);
            return new ConvertProcessor(processorTag, description, field, targetField, convertType, ignoreMissing);
        }
    }
}
