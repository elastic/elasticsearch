/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.ql.execution.search.extractor;

import org.elasticsearch.Version;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.collect.Tuple;
import org.elasticsearch.common.document.DocumentField;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.index.mapper.IgnoredFieldMapper;
import org.elasticsearch.index.mapper.NumberFieldMapper.NumberType;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.xpack.ql.QlIllegalArgumentException;
import org.elasticsearch.xpack.ql.type.DataType;
import org.elasticsearch.xpack.ql.type.DataTypes;

import java.io.IOException;
import java.time.ZoneId;
import java.util.ArrayDeque;
import java.util.Deque;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.StringJoiner;

import static org.elasticsearch.xpack.ql.type.DataTypes.DATETIME;
import static org.elasticsearch.xpack.ql.type.DataTypes.KEYWORD;
import static org.elasticsearch.xpack.ql.type.DataTypes.SCALED_FLOAT;
/**
 * Extractor for ES fields. Works for both 'normal' fields but also nested ones (which require hitName to be set).
 * The latter is used as metadata in assembling the results in the tabular response.
 */
public abstract class AbstractFieldHitExtractor implements HitExtractor {

    private static final Version SWITCHED_FROM_DOCVALUES_TO_SOURCE_EXTRACTION = Version.V_7_4_0;

    /**
     * Source extraction requires only the (relative) field name, without its parent path.
     */
    private static String[] sourcePath(String name, boolean useDocValue, String hitName) {
        return useDocValue ? Strings.EMPTY_ARRAY : Strings
                .tokenizeToStringArray(hitName == null ? name : name.substring(hitName.length() + 1), ".");
    }

    private final String fieldName, hitName;
    private final String fullFieldName; // used to look at the _ignored section of the query response for the actual full field name
    private final DataType dataType;
    private final ZoneId zoneId;
    private final boolean useDocValue;
    private final boolean arrayLeniency;
    private final String[] path;

    protected AbstractFieldHitExtractor(String name, DataType dataType, ZoneId zoneId, boolean useDocValue) {
        this(name, null, dataType, zoneId, useDocValue, null, false);
    }

    protected AbstractFieldHitExtractor(String name, DataType dataType, ZoneId zoneId, boolean useDocValue, boolean arrayLeniency) {
        this(name, null, dataType, zoneId, useDocValue, null, arrayLeniency);
    }

    protected AbstractFieldHitExtractor(String name, String fullFieldName, DataType dataType, ZoneId zoneId, boolean useDocValue,
            String hitName, boolean arrayLeniency) {
        this.fieldName = name;
        this.fullFieldName = fullFieldName;
        this.dataType = dataType;
        this.zoneId = zoneId;
        this.useDocValue = useDocValue;
        this.arrayLeniency = arrayLeniency;
        this.hitName = hitName;

        if (hitName != null) {
            if (!name.contains(hitName)) {
                throw new QlIllegalArgumentException("Hitname [{}] specified but not part of the name [{}]", hitName, name);
            }
        }

        this.path = sourcePath(fieldName, useDocValue, hitName);
    }

    protected AbstractFieldHitExtractor(StreamInput in) throws IOException {
        fieldName = in.readString();
        if (in.getVersion().onOrAfter(SWITCHED_FROM_DOCVALUES_TO_SOURCE_EXTRACTION)) {
            fullFieldName = in.readOptionalString();
        } else {
            fullFieldName = null;
        }
        String typeName = in.readOptionalString();
        dataType = typeName != null ? loadTypeFromName(typeName) : null;
        useDocValue = in.readBoolean();
        hitName = in.readOptionalString();
        arrayLeniency = in.readBoolean();
        path = sourcePath(fieldName, useDocValue, hitName);
        zoneId = readZoneId(in);
    }

    protected DataType loadTypeFromName(String typeName) {
        return DataTypes.fromTypeName(typeName);
    }

    protected abstract ZoneId readZoneId(StreamInput in) throws IOException;

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeString(fieldName);
        if (out.getVersion().onOrAfter(SWITCHED_FROM_DOCVALUES_TO_SOURCE_EXTRACTION)) {
            out.writeOptionalString(fullFieldName);
        }
        out.writeOptionalString(dataType == null ? null : dataType.typeName());
        out.writeBoolean(useDocValue);
        out.writeOptionalString(hitName);
        out.writeBoolean(arrayLeniency);
    }

    @Override
    public Object extract(SearchHit hit) {
        Object value = null;
        if (useDocValue) {
            DocumentField field = hit.field(fieldName);
            if (field != null) {
                value = unwrapMultiValue(field.getValues());
            }
        } else {
            // if the field was ignored because it was malformed and ignore_malformed was turned on
            if (fullFieldName != null
                    && hit.getFields().containsKey(IgnoredFieldMapper.NAME)
                    && isFromDocValuesOnly(dataType) == false
                    && dataType.isNumeric()) {
                /*
                 * ignore_malformed makes sense for extraction from _source for numeric fields only.
                 * And we check here that the data type is actually a numeric one to rule out
                 * any non-numeric sub-fields (for which the "parent" field should actually be extracted from _source).
                 * For example, in the case of a malformed number, a "byte" field with "ignore_malformed: true"
                 * with a "text" sub-field should return "null" for the "byte" parent field and the actual malformed
                 * data for the "text" sub-field. Also, the _ignored section of the response contains the full field
                 * name, thus the need to do the comparison with that and not only the field name.
                 */
                if (hit.getFields().get(IgnoredFieldMapper.NAME).getValues().contains(fullFieldName)) {
                    return null;
                }
            }
            Map<String, Object> source = hit.getSourceAsMap();
            if (source != null) {
                value = extractFromSource(source);
            }
        }
        return value;
    }

    protected Object unwrapMultiValue(Object values) {
        if (values == null) {
            return null;
        }
        if (values instanceof List) {
            List<?> list = (List<?>) values;
            if (list.isEmpty()) {
                return null;
            } else {
                if (isPrimitive(list) == false) {
                    if (list.size() == 1 || arrayLeniency) {
                        return unwrapMultiValue(list.get(0));
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

        // The Jackson json parser can generate for numerics - Integers, Longs, BigIntegers (if Long is not enough)
        // and BigDecimal (if Double is not enough)
        if (values instanceof Number || values instanceof String || values instanceof Boolean) {
            if (dataType == null) {
                return values;
            }
            if (dataType.isNumeric() && isFromDocValuesOnly(dataType) == false) {
                if (dataType == DataTypes.DOUBLE || dataType == DataTypes.FLOAT || dataType == DataTypes.HALF_FLOAT) {
                    Number result = null;
                    try {
                        result = numberType(dataType).parse(values, true);
                    } catch(IllegalArgumentException iae) {
                        return null;
                    }
                    // docvalue_fields is always returning a Double value even if the underlying floating point data type is not Double
                    // even if we don't extract from docvalue_fields anymore, the behavior should be consistent
                    return result.doubleValue();
                } else {
                    Number result = null;
                    try {
                        result = numberType(dataType).parse(values, true);
                    } catch(IllegalArgumentException iae) {
                        return null;
                    }
                    return result;
                }
            } else if (DataTypes.isString(dataType)) {
                return values.toString();
            } else {
                return values;
            }
        }
        throw new QlIllegalArgumentException("Type {} (returned by [{}]) is not supported", values.getClass().getSimpleName(), fieldName);
    }

    protected boolean isFromDocValuesOnly(DataType dataType) {
        return dataType == KEYWORD // because of ignore_above.
                    || dataType == DATETIME
                    || dataType == SCALED_FLOAT; // because of scaling_factor
    }
    
    private static NumberType numberType(DataType dataType) {
        return NumberType.valueOf(dataType.esType().toUpperCase(Locale.ROOT));
    }

    protected abstract Object unwrapCustomValue(Object values);

    protected abstract boolean isPrimitive(List<?> list);

    @SuppressWarnings({ "unchecked", "rawtypes" })
    public Object extractFromSource(Map<String, Object> map) {
        Object value = null;

        // Used to avoid recursive method calls
        // Holds the sub-maps in the document hierarchy that are pending to be inspected along with the current index of the `path`.
        Deque<Tuple<Integer, Map<String, Object>>> queue = new ArrayDeque<>();
        queue.add(new Tuple<>(-1, map));

        while (!queue.isEmpty()) {
            Tuple<Integer, Map<String, Object>> tuple = queue.removeLast();
            int idx = tuple.v1();
            Map<String, Object> subMap = tuple.v2();

            // Find all possible entries by examining all combinations under the current level ("idx") of the "path"
            // e.g.: If the path == "a.b.c.d" and the idx == 0, we need to check the current subMap against the keys:
            //       "b", "b.c" and "b.c.d"
            StringJoiner sj = new StringJoiner(".");
            for (int i = idx + 1; i < path.length; i++) {
                sj.add(path[i]);
                Object node = subMap.get(sj.toString());

                if (node instanceof List) {
                    List listOfValues = (List) node;
                    // we can only do this optimization until the last element of our pass since geo points are using arrays
                    // and we don't want to blindly ignore the second element of array if arrayLeniency is enabled
                    if ((i < path.length - 1) && (listOfValues.size() == 1 || arrayLeniency)) {
                        // this is a List with a size of 1 e.g.: {"a" : [{"b" : "value"}]} meaning the JSON is a list with one element
                        // or a list of values with one element e.g.: {"a": {"b" : ["value"]}}
                        // in case of being lenient about arrays, just extract the first value in the array
                        node = listOfValues.get(0);
                    } else {
                        // a List of elements with more than one value. Break early and let unwrapMultiValue deal with the list
                        return unwrapMultiValue(node);
                    }
                }

                if (node instanceof Map) {
                    if (i < path.length - 1) {
                        // Add the sub-map to the queue along with the current path index
                        queue.add(new Tuple<>(i, (Map<String, Object>) node));
                    } else {
                        // We exhausted the path and got a map
                        // If it is an object - it will be handled in the value extractor
                        value = node;
                    }
                } else if (node != null) {
                    if (i < path.length - 1) {
                        // If we reach a concrete value without exhausting the full path, something is wrong with the mapping
                        // e.g.: map is {"a" : { "b" : "value }} and we are looking for a path: "a.b.c.d"
                        throw new QlIllegalArgumentException("Cannot extract value [{}] from source", fieldName);
                    }
                    if (value != null) {
                        // A value has already been found so this means that there are more than one
                        // values in the document for the same path but different hierarchy.
                        // e.g.: {"a" : {"b" : {"c" : "value"}}}, {"a.b" : {"c" : "value"}}, ...
                        throw new QlIllegalArgumentException("Multiple values (returned by [{}]) are not supported", fieldName);
                    }
                    value = node;
                }
            }
        }
        return unwrapMultiValue(value);
    }

    @Override
    public String hitName() {
        return hitName;
    }

    public String fieldName() {
        return fieldName;
    }

    public String fullFieldName() {
        return fullFieldName;
    }

    public ZoneId zoneId() {
        return zoneId;
    }

    public DataType dataType() {
        return dataType;
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
                && useDocValue == other.useDocValue
                && arrayLeniency == other.arrayLeniency;
    }

    @Override
    public int hashCode() {
        return Objects.hash(fieldName, useDocValue, hitName, arrayLeniency);
    }
}