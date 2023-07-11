/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.ql.execution.search.extractor;

import org.elasticsearch.TransportVersion;
import org.elasticsearch.common.document.DocumentField;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.xpack.ql.QlIllegalArgumentException;
import org.elasticsearch.xpack.ql.type.DataType;
import org.elasticsearch.xpack.ql.type.DataTypes;

import java.io.IOException;
import java.time.ZoneId;
import java.util.ArrayList;
import java.util.Iterator;
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

    protected MultiValueSupport multiValueSupport;

    public enum MultiValueSupport {
        NONE,
        LENIENT,
        FULL
    }

    protected AbstractFieldHitExtractor(String name, DataType dataType, ZoneId zoneId) {
        this(name, dataType, zoneId, null, MultiValueSupport.NONE);
    }

    protected AbstractFieldHitExtractor(String name, DataType dataType, ZoneId zoneId, MultiValueSupport multiValueSupport) {
        this(name, dataType, zoneId, null, multiValueSupport);
    }

    protected AbstractFieldHitExtractor(
        String name,
        DataType dataType,
        ZoneId zoneId,
        String hitName,
        MultiValueSupport multiValueSupport
    ) {
        this.fieldName = name;
        this.dataType = dataType;
        this.zoneId = zoneId;
        this.multiValueSupport = multiValueSupport;
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
        if (in.getTransportVersion().before(TransportVersion.V_8_6_0)) {
            this.multiValueSupport = in.readBoolean() ? MultiValueSupport.LENIENT : MultiValueSupport.NONE;
        } else {
            this.multiValueSupport = in.readEnum(MultiValueSupport.class);
        }
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
        if (out.getTransportVersion().before(TransportVersion.V_8_6_0)) {
            out.writeBoolean(multiValueSupport != MultiValueSupport.NONE);
        } else {
            out.writeEnum(multiValueSupport);
        }

    }

    @Override
    public Object extract(SearchHit hit) {
        Object value = null;
        DocumentField field = null;
        if (hitName != null) {
            value = unwrapFieldsMultiValue(extractNestedField(hit));
        } else {
            field = hit.field(fieldName);
            if (field != null) {
                value = unwrapFieldsMultiValue(field.getValues());
            }
        }
        return value;
    }

    /*
     * For a path of fields like root.nested1.nested2.leaf where nested1 and nested2 are nested field types,
     * fieldName is root.nested1.nested2.leaf, while hitName is root.nested1.nested2
     * We first look for root.nested1.nested2 or root.nested1 or root in the SearchHit until we find something.
     * If the DocumentField lives under "root.nested1" the remaining path to search for (in the DocumentField itself) is nested2.
     * After this step is done, what remains to be done is just getting the leaf values.
     */
    @SuppressWarnings("unchecked")
    private Object extractNestedField(SearchHit hit) {
        Object value;
        DocumentField field;
        String tempHitname = hitName;
        List<String> remainingPath = new ArrayList<>();
        // first, search for the "root" DocumentField under which the remaining path of nested document values is
        while ((field = hit.field(tempHitname)) == null) {
            int indexOfDot = tempHitname.lastIndexOf(".");
            if (indexOfDot < 0) {// there is no such field in the hit
                return null;
            }
            remainingPath.add(0, tempHitname.substring(indexOfDot + 1));
            tempHitname = tempHitname.substring(0, indexOfDot);
        }
        // then dig into DocumentField's structure until we reach the field we are interested into
        if (remainingPath.size() > 0) {
            List<Object> values = field.getValues();
            Iterator<String> pathIterator = remainingPath.iterator();
            while (pathIterator.hasNext()) {
                String pathElement = pathIterator.next();
                Map<String, List<Object>> elements = (Map<String, List<Object>>) values.get(0);
                values = elements.get(pathElement);
                /*
                 * if this path is not found it means we hit another nested document (inner_root_1.inner_root_2.nested_field_2)
                 * something like this
                 * "root_field_1.root_field_2.nested_field_1" : [
                 *     {
                 *       "inner_root_1.inner_root_2.nested_field_2" : [
                 *         {
                 *           "leaf_field" : [
                 *             "abc2"
                 *           ]
                 * So, start re-building the path until the right one is found, ie inner_root_1.inner_root_2......
                 */
                while (values == null) {
                    pathElement += "." + pathIterator.next();
                    values = elements.get(pathElement);
                }
            }
            value = ((Map<String, Object>) values.get(0)).get(fieldName.substring(hitName.length() + 1));
        } else {
            value = field.getValues();
        }
        return value;
    }

    protected Object unwrapFieldsMultiValue(Object values) {
        if (values == null) {
            return null;
        }
        if (values instanceof Map && hitName != null) {
            // extract the sub-field from a nested field (dep.dep_name -> dep_name)
            return unwrapFieldsMultiValue(((Map<?, ?>) values).get(fieldName.substring(hitName.length() + 1)));
        }
        if (values instanceof List<?> list) {
            if (list.isEmpty()) {
                return null;
            } else {
                if (isPrimitive(list) == false) {
                    if (list.size() == 1 || multiValueSupport == MultiValueSupport.LENIENT) {
                        return unwrapFieldsMultiValue(list.get(0));
                    } else if (multiValueSupport == MultiValueSupport.FULL) {
                        List<Object> unwrappedValues = new ArrayList<>();
                        for (Object value : list) {
                            unwrappedValues.add(unwrapFieldsMultiValue(value));
                        }
                        values = unwrappedValues;
                    } else {
                        throw new QlIllegalArgumentException("Arrays (returned by [{}]) are not supported", fieldName);
                    }
                }
            }
        }

        Object unwrapped = unwrapCustomValue(values);
        if (unwrapped != null && isListOfNulls(unwrapped) == false) {
            return unwrapped;
        }

        return values;
    }

    private static boolean isListOfNulls(Object unwrapped) {
        if (unwrapped instanceof List<?> list) {
            if (list.size() == 0) {
                return false;
            }
            for (Object o : list) {
                if (o != null) {
                    return false;
                }
            }
            return true;
        }
        return false;
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

    public MultiValueSupport multiValueSupport() {
        return multiValueSupport;
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
        return fieldName.equals(other.fieldName) && hitName.equals(other.hitName) && multiValueSupport == other.multiValueSupport;
    }

    @Override
    public int hashCode() {
        return Objects.hash(fieldName, hitName, multiValueSupport);
    }
}
