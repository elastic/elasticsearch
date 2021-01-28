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
import java.util.ArrayList;
import java.util.Deque;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.StringJoiner;

import static java.util.Collections.singletonList;
import static org.elasticsearch.xpack.ql.type.DataTypes.DATETIME;
import static org.elasticsearch.xpack.ql.type.DataTypes.KEYWORD;
import static org.elasticsearch.xpack.ql.type.DataTypes.SCALED_FLOAT;

/**
 * Extractor for ES fields. Works for both 'normal' fields but also nested ones (which require hitName to be set).
 * The latter is used as metadata in assembling the results in the tabular response.
 */
public abstract class AbstractFieldHitExtractor implements HitExtractor {

    private static final Version SWITCHED_FROM_DOCVALUES_TO_SOURCE_EXTRACTION = Version.V_7_4_0;
    private static final Version INTRODUCED_MULTI_VALUE_EXTRACTION = Version.V_7_12_0; // TODO: update if merging in 7.13.0

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
                return object instanceof List ? object : singletonList(object);
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
    private final MultiValueHandling multiValueHandling;
    private final String[] path;

    protected AbstractFieldHitExtractor(String name, DataType dataType, ZoneId zoneId, boolean useDocValue) {
        this(name, null, dataType, zoneId, useDocValue, null, MultiValueHandling.FAIL_IF_MULTIVALUE);
    }

    protected AbstractFieldHitExtractor(String name, DataType dataType, ZoneId zoneId, boolean useDocValue,
                                        MultiValueHandling multiValueHandling) {
        this(name, null, dataType, zoneId, useDocValue, null, multiValueHandling);
    }

    protected AbstractFieldHitExtractor(String name, String fullFieldName, DataType dataType, ZoneId zoneId, boolean useDocValue,
            String hitName, MultiValueHandling multiValueHandling) {
        this.fieldName = name;
        this.fullFieldName = fullFieldName;
        this.dataType = dataType;
        this.zoneId = zoneId;
        this.useDocValue = useDocValue;
        this.multiValueHandling = multiValueHandling;
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
        if (in.getVersion().onOrAfter(INTRODUCED_MULTI_VALUE_EXTRACTION)) {
            multiValueHandling = in.readEnum(MultiValueHandling.class);
        } else {
            multiValueHandling = in.readBoolean() ? MultiValueHandling.EXTRACT_ONE : MultiValueHandling.FAIL_IF_MULTIVALUE;
        }
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
        if (out.getVersion().onOrAfter(INTRODUCED_MULTI_VALUE_EXTRACTION)) {
            out.writeEnum(multiValueHandling);
        } else {
            out.writeBoolean(multiValueHandling != MultiValueHandling.FAIL_IF_MULTIVALUE);
        }
    }

    @Override
    public Object extract(SearchHit hit) {
        Object value = null;
        if (useDocValue) {
            DocumentField field = hit.field(fieldName);
            if (field != null) {
                value = extractMultiValue(field.getValues());
            }
        } else {
            // if the field was ignored because it was malformed and ignore_malformed was turned on
            if (fullFieldName != null
                    && hit.getFields().containsKey(IgnoredFieldMapper.NAME)
                    && isFromDocValuesOnly(dataType) == false) {
                /*
                 * We check here the presence of the field name (fullFieldName including the parent name) in the list
                 * of _ignored fields (due to malformed data, which was ignored).
                 * For example, in the case of a malformed number, a "byte" field with "ignore_malformed: true"
                 * with a "text" sub-field should return "null" for the "byte" parent field and the actual malformed
                 * data for the "text" sub-field.
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

    protected Object extractMultiValue(Object values) {
        return multiValueHandling.handle(unwrapMultiValue(values), fieldName);
    }

    protected Object unwrapMultiValue(Object values) {
        if (values == null) {
            return null;
        }
        if (values instanceof List) {
            List<?> list = (List<?>) values;
            if (isPrimitive(list) == false) {
                List<Object> unwrappedList = new ArrayList<>();
                for (Object o : list) {
                    Object unwrapped = unwrapMultiValue(o);
                    if (unwrapped instanceof List) {
                        unwrappedList.addAll((List<?>) unwrapped);
                    } else {
                        unwrappedList.add(unwrapped);
                    }
                }
                return unwrappedList;
            }
        }

        Object unwrapped = unwrapCustomValue(values);
        if (unwrapped != null) {
            return unwrapped;
        }

        // The Jackson json parser can generate for numerics - Integers, Longs, BigIntegers (if Long is not enough)
        // and BigDecimal (if Double is not enough)
        if (values instanceof Number || values instanceof String || values instanceof Boolean) {
            return unwrapNumberOrStringOrBoolean(values);
        }

        throw new QlIllegalArgumentException("Type {} (returned by [{}]) is not supported", values.getClass().getSimpleName(), fieldName);
    }

    private Object unwrapNumberOrStringOrBoolean(Object value) {
        if (dataType == null) {
            return value;
        }
        if (dataType.isNumeric() && isFromDocValuesOnly(dataType) == false) {
            if (dataType == DataTypes.DOUBLE || dataType == DataTypes.FLOAT || dataType == DataTypes.HALF_FLOAT) {
                Number result = null;
                try {
                    result = numberType(dataType).parse(value, true);
                } catch(IllegalArgumentException iae) {
                    return null;
                }
                // docvalue_fields is always returning a Double value even if the underlying floating point data type is not Double
                // even if we don't extract from docvalue_fields anymore, the behavior should be consistent
                return result.doubleValue();
            } else {
                Number result = null;
                try {
                    result = numberType(dataType).parse(value, true);
                } catch(IllegalArgumentException iae) {
                    return null;
                }
                return result;
            }
        } else if (DataTypes.isString(dataType) || dataType == DataTypes.IP) {
            return value.toString();
        } else {
            return value;
        }
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
        List<Object> values = new ArrayList<>();

        // Used to avoid recursive method calls
        // Holds the sub-maps in the document hierarchy that are pending to be inspected along with the current index of the `path`.
        Deque<Tuple<Integer, Map<String, Object>>> queue = new ArrayDeque<>();
        queue.add(new Tuple<>(-1, map));

        while (!queue.isEmpty()) {
            Tuple<Integer, Map<String, Object>> tuple = queue.removeFirst();
            int idx = tuple.v1();
            Map<String, Object> subMap = tuple.v2();

            // Find all possible entries by examining all combinations under the current level ("idx") of the "path"
            // e.g.: If the path == "a.b.c.d" and the idx == 0, we need to check the current subMap against the keys:
            //       "b", "b.c" and "b.c.d"
            StringJoiner sj = new StringJoiner(".");
            for (int i = idx + 1; i < path.length; i++) {
                sj.add(path[i]);
                String currentPath = sj.toString();
                // need to differentiate between Map#get() returning null b/c there's no mapping VS the mapping's actual value
                if (subMap.containsKey(currentPath)) {
                    Object node = subMap.get(currentPath);

                    if (node instanceof List) { // {"a": {"b": [...]}}
                        List listOfValues = (List) node;
                        // if the path is not yet exhausted (ex. at "a.b.c" level for a "a.b.c.d" path), queue whatever's in the list and
                        // inspect it with next outer loop iteration.
                        if (i < path.length - 1) {
                            final int level = i;
                            listOfValues.forEach(o -> {
                                if (o instanceof Map) {
                                    queue.add(new Tuple<>(level, (Map<String, Object>) o));
                                } else {
                                    // another list or an "end"/concrete value: smth is wrong, either with the mapping or with the map/doc
                                    throw new QlIllegalArgumentException("Cannot extract field [{}] value [{}] from source", fieldName,
                                        node);
                                }
                            });
                        }
                        // if the path is exhausted, just add the list to the output list and let extractMultiValue & co deal with it
                        else {
                            values.add(node);
                        }
                    } else if (node instanceof Map) {
                        if (i < path.length - 1) {
                            // Add the sub-map to the queue along with the current path index
                            queue.add(new Tuple<>(i, (Map<String, Object>) node));
                        } else {
                            // We exhausted the path and got a map
                            // If it is an object - it will be handled in the value extractor
                            values.add(node);
                        }
                    } else {
                        if (i < path.length - 1) {
                            if (node != null) {
                                // If we reach a concrete value without exhausting the full path, something is wrong with the mapping
                                // e.g.: map is {"a" : { "b" : "value }} and we are looking for a path: "a.b.c.d"
                                throw new QlIllegalArgumentException("Cannot extract value [{}] from source", fieldName);
                            }
                        } else {
                            values.add(node);
                        }
                    }
                }
            }
        }
        return extractMultiValue(values);
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

    public boolean useDocValues() {
        return useDocValue;
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
                && useDocValue == other.useDocValue
                && multiValueHandling == other.multiValueHandling;
    }

    @Override
    public int hashCode() {
        return Objects.hash(fieldName, useDocValue, hitName, multiValueHandling);
    }
}
