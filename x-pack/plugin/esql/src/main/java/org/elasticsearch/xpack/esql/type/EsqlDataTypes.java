/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.esql.type;

import org.elasticsearch.xpack.ql.type.DataType;
import org.elasticsearch.xpack.ql.type.DataTypes;
import org.elasticsearch.xpack.ql.type.EsField;
import org.elasticsearch.xpack.ql.type.KeywordEsField;
import org.elasticsearch.xpack.ql.type.UnsupportedEsField;

import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.Map;
import java.util.TreeMap;

import static java.util.stream.Collectors.toMap;
import static java.util.stream.Collectors.toUnmodifiableMap;
import static org.elasticsearch.xpack.ql.type.DataTypes.BOOLEAN;
import static org.elasticsearch.xpack.ql.type.DataTypes.BYTE;
import static org.elasticsearch.xpack.ql.type.DataTypes.DATETIME;
import static org.elasticsearch.xpack.ql.type.DataTypes.DOUBLE;
import static org.elasticsearch.xpack.ql.type.DataTypes.FLOAT;
import static org.elasticsearch.xpack.ql.type.DataTypes.HALF_FLOAT;
import static org.elasticsearch.xpack.ql.type.DataTypes.INTEGER;
import static org.elasticsearch.xpack.ql.type.DataTypes.KEYWORD;
import static org.elasticsearch.xpack.ql.type.DataTypes.LONG;
import static org.elasticsearch.xpack.ql.type.DataTypes.NESTED;
import static org.elasticsearch.xpack.ql.type.DataTypes.NULL;
import static org.elasticsearch.xpack.ql.type.DataTypes.OBJECT;
import static org.elasticsearch.xpack.ql.type.DataTypes.SCALED_FLOAT;
import static org.elasticsearch.xpack.ql.type.DataTypes.SHORT;
import static org.elasticsearch.xpack.ql.type.DataTypes.UNSUPPORTED;

public final class EsqlDataTypes {

    public static final DataType DATE_PERIOD = new DataType("DATE_PERIOD", null, 3 * Integer.BYTES, false, false, false);
    public static final DataType TIME_DURATION = new DataType("TIME_DURATION", null, Integer.BYTES + Long.BYTES, false, false, false);

    private static final Collection<DataType> TYPES = Arrays.asList(
        BOOLEAN,
        UNSUPPORTED,
        NULL,
        INTEGER,
        LONG,
        DOUBLE,
        KEYWORD,
        DATETIME,
        DATE_PERIOD,
        TIME_DURATION,
        SCALED_FLOAT
    ).stream().sorted(Comparator.comparing(DataType::typeName)).toList();

    private static final Map<String, DataType> NAME_TO_TYPE = TYPES.stream().collect(toUnmodifiableMap(DataType::typeName, t -> t));

    private static Map<String, DataType> ES_TO_TYPE;

    static {
        Map<String, DataType> map = TYPES.stream().filter(e -> e.esType() != null).collect(toMap(DataType::esType, t -> t));
        ES_TO_TYPE = Collections.unmodifiableMap(map);
    }

    private EsqlDataTypes() {}

    public static Collection<DataType> types() {
        return TYPES;
    }

    public static DataType fromEs(String name) {
        DataType type = ES_TO_TYPE.get(name);
        return type != null ? type : UNSUPPORTED;
    }

    public static DataType fromJava(Object value) {
        if (value == null) {
            return NULL;
        }
        if (value instanceof Boolean) {
            return BOOLEAN;
        }
        if (value instanceof Integer) {
            return INTEGER;
        }
        if (value instanceof Long) {
            return LONG;
        }
        if (value instanceof Double) {
            return DOUBLE;
        }
        if (value instanceof Float) {
            return FLOAT;
        }
        if (value instanceof String || value instanceof Character) {
            return KEYWORD;
        }

        return null;
    }

    public static boolean isUnsupported(DataType from) {
        return from == UNSUPPORTED || from == NESTED || from == OBJECT;
    }

    public static boolean isString(DataType t) {
        return t == KEYWORD;
    }

    public static boolean isPrimitive(DataType t) {
        return t != OBJECT && t != NESTED && t != UNSUPPORTED;
    }

    public static boolean areCompatible(DataType left, DataType right) {
        if (left == right) {
            return true;
        } else {
            return (left == NULL || right == NULL) || (isString(left) && isString(right)) || (left.isNumeric() && right.isNumeric());
        }
    }

    public static void filterUnsupportedDataTypes(Map<String, EsField> oldFields, Map<String, EsField> newFields) {
        for (Map.Entry<String, EsField> entry : oldFields.entrySet()) {
            EsField field = entry.getValue();
            Map<String, EsField> subFields = field.getProperties();
            DataType fieldType = promoteToSupportedType(field.getDataType());
            if (subFields.isEmpty()) {
                if (isSupportedDataType(fieldType)) {
                    newFields.put(entry.getKey(), field.withType(fieldType));
                }
            } else {
                String name = field.getName();
                Map<String, EsField> newSubFields = new TreeMap<>();

                filterUnsupportedDataTypes(subFields, newSubFields);
                if (isSupportedDataType(fieldType)) {
                    newFields.put(entry.getKey(), new EsField(name, fieldType, newSubFields, field.isAggregatable(), field.isAlias()));
                }
                // unsupported field having supported sub-fields, except NESTED (which we'll ignore completely)
                else if (newSubFields.isEmpty() == false && fieldType != DataTypes.NESTED) {
                    // mark the fields itself as unsupported, but keep its supported subfields
                    newFields.put(entry.getKey(), new UnsupportedEsField(name, fieldType.typeName(), null, newSubFields));
                }
            }
        }
    }

    private static DataType promoteToSupportedType(DataType type) {
        if (type == BYTE || type == SHORT) {
            return INTEGER;
        }
        if (type == HALF_FLOAT || type == FLOAT || type == SCALED_FLOAT) {
            return DOUBLE;
        }
        return type;
    }

    public static boolean isSupportedDataType(DataType type) {
        return isUnsupported(type) == false && types().contains(type);
    }

    public static Map<String, EsField> flatten(Map<String, EsField> mapping) {
        TreeMap<String, EsField> newMapping = new TreeMap<>();
        flatten(mapping, null, newMapping);
        return newMapping;
    }

    public static void flatten(Map<String, EsField> mapping, String parentName, Map<String, EsField> newMapping) {
        for (Map.Entry<String, EsField> entry : mapping.entrySet()) {
            String name = entry.getKey();
            EsField t = entry.getValue();

            if (t != null) {
                String fullName = parentName == null ? name : parentName + "." + name;
                var fieldProperties = t.getProperties();
                if (t instanceof UnsupportedEsField == false) {
                    if (fieldProperties.isEmpty()) {
                        // use the field's full name instead
                        newMapping.put(fullName, t);
                    } else {
                        // use the field's full name and an empty list of subfields (each subfield will be created separately from its
                        // parent)
                        if (t instanceof KeywordEsField kef) {
                            newMapping.put(
                                fullName,
                                new KeywordEsField(fullName, Map.of(), kef.isAggregatable(), kef.getPrecision(), false, kef.isAlias())
                            );
                        } else {
                            newMapping.put(fullName, new EsField(fullName, t.getDataType(), Map.of(), t.isAggregatable(), t.isAlias()));
                        }
                    }
                }
                if (fieldProperties.isEmpty() == false) {
                    flatten(fieldProperties, fullName, newMapping);
                }
            }
        }
    }
}
