/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.ql.execution.search.extractor;

import org.elasticsearch.common.document.DocumentField;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.xpack.ql.QlIllegalArgumentException;
import org.elasticsearch.xpack.ql.type.DataType;
import org.elasticsearch.xpack.ql.type.DataTypes;

import java.io.IOException;
import java.time.ZoneId;
import java.util.List;
import java.util.Map;
import java.util.Objects;

/**
 * Extractor for ES fields. Works for both 'normal' fields but also nested ones (which require hitName to be set).
 * The latter is used as metadata in assembling the results in the tabular response.
 */
public abstract class AbstractFieldHitExtractor implements HitExtractor {

    private final String fieldName, hitName;
    private final DataType dataType;
    private final ZoneId zoneId;
    private final boolean arrayLeniency;

    protected AbstractFieldHitExtractor(String name, DataType dataType, ZoneId zoneId) {
        this(name, dataType, zoneId, null, false);
    }

    protected AbstractFieldHitExtractor(String name, DataType dataType, ZoneId zoneId, boolean arrayLeniency) {
        this(name, dataType, zoneId, null, arrayLeniency);
    }

    protected AbstractFieldHitExtractor(String name, DataType dataType, ZoneId zoneId, String hitName, boolean arrayLeniency) {
        this.fieldName = name;
        this.dataType = dataType;
        this.zoneId = zoneId;
        this.arrayLeniency = arrayLeniency;
        this.hitName = hitName;

        if (hitName != null) {
            if (name.contains(hitName) == false) {
                throw new QlIllegalArgumentException("Hitname [{}] specified but not part of the name [{}]", hitName, name);
            }
        }
    }

    protected AbstractFieldHitExtractor(StreamInput in) throws IOException {
        fieldName = in.readString();
        String typeName = in.readOptionalString();
        dataType = typeName != null ? loadTypeFromName(typeName) : null;
        hitName = in.readOptionalString();
        arrayLeniency = in.readBoolean();
        zoneId = readZoneId(in);
    }

    protected DataType loadTypeFromName(String typeName) {
        return DataTypes.fromTypeName(typeName);
    }

    protected abstract ZoneId readZoneId(StreamInput in) throws IOException;

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeString(fieldName);
        out.writeOptionalString(dataType == null ? null : dataType.typeName());
        out.writeOptionalString(hitName);
        out.writeBoolean(arrayLeniency);
    }

    @Override
    public Object extract(SearchHit hit) {
        Object value = null;
        DocumentField field = null;
        if (hitName != null) {
            // a nested field value is grouped under the nested parent name (ie dep.dep_name lives under "dep":[{dep_name:value}])
            field = hit.field(hitName);
        } else {
            field = hit.field(fieldName);
        }
        if (field != null) {
            value = unwrapFieldsMultiValue(field.getValues());
        }
        return value;
    }

    protected Object unwrapFieldsMultiValue(Object values) {
        if (values == null) {
            return null;
        }
        if (values instanceof Map && hitName != null) {
            // extract the sub-field from a nested field (dep.dep_name -> dep_name)
            return unwrapFieldsMultiValue(((Map<?,?>) values).get(fieldName.substring(hitName.length() + 1)));
        }
        if (values instanceof List) {
            List<?> list = (List<?>) values;
            if (list.isEmpty()) {
                return null;
            } else {
                if (isPrimitive(list) == false) {
                    if (list.size() == 1 || arrayLeniency) {
                        return unwrapFieldsMultiValue(list.get(0));
                    } else {
                        throw new QlIllegalArgumentException("Arrays (returned by [{}]) are not supported", fieldName);
                    }
                }
            }
        }

        Object unwrapped = unwrapCustomValue(values);
        if (unwrapped != null) {
            return unwrapped;
        }

        return values;
    }

    protected abstract Object unwrapCustomValue(Object values);

    protected abstract boolean isPrimitive(List<?> list);

    @Override
    public String hitName() {
        return hitName;
    }

    public String fieldName() {
        return fieldName;
    }

    public ZoneId zoneId() {
        return zoneId;
    }

    public DataType dataType() {
        return dataType;
    }

    public boolean arrayLeniency() {
        return arrayLeniency;
    }

    @Override
    public String toString() {
        return fieldName + "@" + hitName + "@" + zoneId;
    }

    @Override
    public boolean equals(Object obj) {
        if (obj == null || obj.getClass() != getClass()) {
            return false;
        }
        AbstractFieldHitExtractor other = (AbstractFieldHitExtractor) obj;
        return fieldName.equals(other.fieldName)
                && hitName.equals(other.hitName)
                && arrayLeniency == other.arrayLeniency;
    }

    @Override
    public int hashCode() {
        return Objects.hash(fieldName, hitName, arrayLeniency);
    }
}