/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.ql.execution.search.extractor;

import org.elasticsearch.Version;
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

import static java.util.Collections.emptyList;
import static java.util.Collections.singletonList;

/**
 * Extractor for ES fields. Works for both 'normal' fields but also nested ones (which require hitName to be set).
 * The latter is used as metadata in assembling the results in the tabular response.
 */
public abstract class AbstractFieldHitExtractor implements HitExtractor {

    private static final Version INTRODUCED_MULTI_VALUE_EXTRACTION = Version.V_7_15_0; // TODO: update if merging in 7.16.0

    public enum MultiValueHandling {
        FAIL_IF_MULTIVALUE {
            @Override
            public Object handle(Object object, String fieldName) {
                return extractOneValue(object, fieldName, true);
            }
        },
        EXTRACT_ONE {
            @Override
            public Object handle(Object object, String fieldName) {
                return extractOneValue(object, fieldName, false);
            }
        },
        EXTRACT_ARRAY {
            @Override
            public Object handle(Object object, String _ignored) {
                return object == null ? emptyList() : (object instanceof List ? object : singletonList(object));
            }
        };

        public abstract Object handle(Object unwrapped, String fieldName);

        private static Object extractOneValue(Object object, String fieldName, boolean failIfMultiValue) {
            if (object instanceof List) { // is this a multi-value?
                List<?> list = (List<?>) object;
                if (list.size() > 1 && failIfMultiValue) {
                    throw new QlIllegalArgumentException("Cannot return multiple values for field [{}]; use ARRAY({}) instead",
                        fieldName, fieldName);
                }
                return list.isEmpty() ? null : list.get(0);
            }
            return object;
        }
    }

    private final String fieldName, hitName;
    private final DataType dataType;
    private final ZoneId zoneId;
    private final MultiValueHandling multiValueHandling;

    protected AbstractFieldHitExtractor(String name, DataType dataType, ZoneId zoneId) {
        this(name, dataType, zoneId, null, MultiValueHandling.FAIL_IF_MULTIVALUE);
    }

    protected AbstractFieldHitExtractor(String name, DataType dataType, ZoneId zoneId, MultiValueHandling multiValueHandling) {
        this(name, dataType, zoneId, null, multiValueHandling);
    }

    protected AbstractFieldHitExtractor(String name, DataType dataType, ZoneId zoneId, String hitName,
        MultiValueHandling multiValueHandling) {
        this.fieldName = name;
        this.dataType = dataType;
        this.zoneId = zoneId;
        this.multiValueHandling = multiValueHandling;
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
        if (in.getVersion().onOrAfter(INTRODUCED_MULTI_VALUE_EXTRACTION)) {
            multiValueHandling = in.readEnum(MultiValueHandling.class);
        } else {
            multiValueHandling = in.readBoolean() ? MultiValueHandling.EXTRACT_ONE : MultiValueHandling.FAIL_IF_MULTIVALUE;
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
        if (out.getVersion().onOrAfter(INTRODUCED_MULTI_VALUE_EXTRACTION)) {
            out.writeEnum(multiValueHandling);
        } else {
            out.writeBoolean(multiValueHandling != MultiValueHandling.FAIL_IF_MULTIVALUE);
        }
    }

    @Override
    public Object extract(SearchHit hit) {
        Object value = null;
        String lookupName = null;
        DocumentField field = null;
        if (hitName != null) {
            value = unwrapFieldsMultiValue(extractNestedField(hit));
            lookupName = hitName;
        } else {
            field = hit.field(fieldName);
            if (field != null) {
                value = unwrapFieldsMultiValue(field.getValues());
            }
            lookupName = fieldName;
        }
        return multiValueHandling.handle(value, lookupName);
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
            return unwrapFieldsMultiValue(((Map<?,?>) values).get(fieldName.substring(hitName.length() + 1)));
        }
        if (values instanceof List) {
            List<?> list = (List<?>) values;
            if (list.isEmpty()) {
                return null;
            } else {
                if (isPrimitive(list) == false) {
                    List<Object> unwrappedList = new ArrayList<>();
                    for (Object o : list) {
                        Object unwrapped = unwrapFieldsMultiValue(o);
                        if (unwrapped instanceof List) {
                            unwrappedList.addAll((List<?>) unwrapped);
                        } else {
                            unwrappedList.add(unwrapped);
                        }
                    }
                    return unwrappedList;
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

    public MultiValueHandling multiValueExtraction() {
        return multiValueHandling;
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
                && multiValueHandling == other.multiValueHandling;
    }

    @Override
    public int hashCode() {
        return Objects.hash(fieldName, hitName, multiValueHandling);
    }
}
