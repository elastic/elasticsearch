/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.ingest;

import org.elasticsearch.common.Strings;
import org.elasticsearch.common.util.LazyMap;
import org.elasticsearch.common.util.Maps;
import org.elasticsearch.common.util.set.Sets;
import org.elasticsearch.index.VersionType;
import org.elasticsearch.index.mapper.IdFieldMapper;
import org.elasticsearch.index.mapper.IndexFieldMapper;
import org.elasticsearch.index.mapper.RoutingFieldMapper;
import org.elasticsearch.index.mapper.SourceFieldMapper;
import org.elasticsearch.index.mapper.VersionFieldMapper;
import org.elasticsearch.script.CtxMap;
import org.elasticsearch.script.TemplateScript;

import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Base64;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.function.BiConsumer;
import java.util.stream.Collectors;

/**
 * Represents a single document being captured before indexing and holds the source and metadata (like id, type and index).
 */
public final class IngestDocument {

    public static final String INGEST_KEY = "_ingest";
    public static final String PIPELINE_CYCLE_ERROR_MESSAGE = "Cycle detected for pipeline: ";
    private static final String INGEST_KEY_PREFIX = INGEST_KEY + ".";
    private static final String SOURCE_PREFIX = SourceFieldMapper.NAME + ".";

    static final String TIMESTAMP = "timestamp";

    private final IngestCtxMap ctxMap;
    private final Map<String, Object> ingestMetadata;

    // Contains all pipelines that have been executed for this document
    private final Set<String> executedPipelines = new LinkedHashSet<>();

    private boolean doNoSelfReferencesCheck = false;

    public IngestDocument(String index, String id, long version, String routing, VersionType versionType, Map<String, Object> source) {
        this.ctxMap = new IngestCtxMap(index, id, version, routing, versionType, ZonedDateTime.now(ZoneOffset.UTC), source);
        this.ingestMetadata = new HashMap<>();
        this.ingestMetadata.put(TIMESTAMP, ctxMap.getMetadata().getNow());
    }

    /**
     * Copy constructor that creates a new {@link IngestDocument} which has exactly the same properties as the one provided as argument
     */
    public IngestDocument(IngestDocument other) {
        this(
            new IngestCtxMap(deepCopyMap(other.ctxMap.getSource()), other.ctxMap.getMetadata().clone()),
            deepCopyMap(other.ingestMetadata)
        );
    }

    /**
     * Constructor to create an IngestDocument from its constituent maps.  The maps are shallow copied.
     */
    public IngestDocument(Map<String, Object> sourceAndMetadata, Map<String, Object> ingestMetadata) {
        Map<String, Object> source;
        Map<String, Object> metadata;
        if (sourceAndMetadata instanceof IngestCtxMap ingestCtxMap) {
            source = new HashMap<>(ingestCtxMap.getSource());
            metadata = new HashMap<>(ingestCtxMap.getMetadata().getMap());
        } else {
            metadata = Maps.newHashMapWithExpectedSize(Metadata.METADATA_NAMES.size());
            source = new HashMap<>(sourceAndMetadata);
            for (String key : Metadata.METADATA_NAMES) {
                if (sourceAndMetadata.containsKey(key)) {
                    metadata.put(key, source.remove(key));
                }
            }
        }
        this.ingestMetadata = new HashMap<>(ingestMetadata);
        this.ctxMap = new IngestCtxMap(source, new IngestDocMetadata(metadata, IngestCtxMap.getTimestamp(ingestMetadata)));
    }

    /**
     * Constructor to create an IngestDocument from its constituent maps
     */
    IngestDocument(IngestCtxMap ctxMap, Map<String, Object> ingestMetadata) {
        this.ctxMap = ctxMap;
        this.ingestMetadata = ingestMetadata;
    }

    /**
     * Returns the value contained in the document for the provided path
     * @param path The path within the document in dot-notation
     * @param clazz The expected class of the field value
     * @return the value for the provided path if existing
     * @throws IllegalArgumentException if the path is null, empty, invalid, if the field doesn't exist
     * or if the field that is found at the provided path is not of the expected type.
     */
    public <T> T getFieldValue(String path, Class<T> clazz) {
        return getFieldValue(path, clazz, false);
    }

    /**
     * Returns the value contained in the document for the provided path
     *
     * @param path The path within the document in dot-notation
     * @param clazz The expected class of the field value
     * @param ignoreMissing The flag to determine whether to throw an exception when `path` is not found in the document.
     * @return the value for the provided path if existing, null otherwise.
     * @throws IllegalArgumentException only if ignoreMissing is false and the path is null, empty, invalid, if the field doesn't exist
     * or if the field that is found at the provided path is not of the expected type.
     */
    public <T> T getFieldValue(String path, Class<T> clazz, boolean ignoreMissing) {
        FieldPath fieldPath = new FieldPath(path);
        Object context = fieldPath.initialContext;
        for (String pathElement : fieldPath.pathElements) {
            ResolveResult result = resolve(pathElement, path, context);
            if (result.wasSuccessful) {
                context = result.resolvedObject;
            } else if (ignoreMissing && hasField(path) == false) {
                return null;
            } else {
                throw new IllegalArgumentException(result.errorMessage);
            }
        }
        return cast(path, context, clazz);
    }

    /**
     * Returns the value contained in the document with the provided templated path
     * @param pathTemplate The path within the document in dot-notation
     * @param clazz The expected class fo the field value
     * @return the value fro the provided path if existing, null otherwise
     * @throws IllegalArgumentException if the pathTemplate is null, empty, invalid, if the field doesn't exist,
     * or if the field that is found at the provided path is not of the expected type.
     */
    public <T> T getFieldValue(TemplateScript.Factory pathTemplate, Class<T> clazz) {
        return getFieldValue(renderTemplate(pathTemplate), clazz);
    }

    /**
     * Returns the value contained in the document for the provided path as a byte array.
     * If the path value is a string, a base64 decode operation will happen.
     * If the path value is a byte array, it is just returned
     * @param path The path within the document in dot-notation
     * @return the byte array for the provided path if existing
     * @throws IllegalArgumentException if the path is null, empty, invalid, if the field doesn't exist
     * or if the field that is found at the provided path is not of the expected type.
     */
    public byte[] getFieldValueAsBytes(String path) {
        return getFieldValueAsBytes(path, false);
    }

    /**
     * Returns the value contained in the document for the provided path as a byte array.
     * If the path value is a string, a base64 decode operation will happen.
     * If the path value is a byte array, it is just returned
     * @param path The path within the document in dot-notation
     * @param ignoreMissing The flag to determine whether to throw an exception when `path` is not found in the document.
     * @return the byte array for the provided path if existing
     * @throws IllegalArgumentException if the path is null, empty, invalid, if the field doesn't exist
     * or if the field that is found at the provided path is not of the expected type.
     */
    public byte[] getFieldValueAsBytes(String path, boolean ignoreMissing) {
        Object object = getFieldValue(path, Object.class, ignoreMissing);
        if (object == null) {
            return null;
        } else if (object instanceof byte[] bytes) {
            return bytes;
        } else if (object instanceof String string) {
            return Base64.getDecoder().decode(string);
        } else {
            throw new IllegalArgumentException(
                "Content field [" + path + "] of unknown type [" + object.getClass().getName() + "], must be string or byte array"
            );
        }
    }

    /**
     * Checks whether the document contains a value for the provided templated path
     * @param fieldPathTemplate the template for the path within the document in dot-notation
     * @return true if the document contains a value for the field, false otherwise
     * @throws IllegalArgumentException if the path is null, empty or invalid
     */
    public boolean hasField(TemplateScript.Factory fieldPathTemplate) {
        return hasField(renderTemplate(fieldPathTemplate));
    }

    /**
     * Checks whether the document contains a value for the provided path
     * @param path The path within the document in dot-notation
     * @return true if the document contains a value for the field, false otherwise
     * @throws IllegalArgumentException if the path is null, empty or invalid.
     */
    public boolean hasField(String path) {
        return hasField(path, false);
    }

    /**
     * Checks whether the document contains a value for the provided path
     * @param path The path within the document in dot-notation
     * @param failOutOfRange Whether to throw an IllegalArgumentException if array is accessed outside of its range
     * @return true if the document contains a value for the field, false otherwise
     * @throws IllegalArgumentException if the path is null, empty or invalid.
     */
    public boolean hasField(String path, boolean failOutOfRange) {
        FieldPath fieldPath = new FieldPath(path);
        Object context = fieldPath.initialContext;
        for (int i = 0; i < fieldPath.pathElements.length - 1; i++) {
            String pathElement = fieldPath.pathElements[i];
            if (context == null) {
                return false;
            }
            if (context instanceof Map<?, ?> map) {
                context = map.get(pathElement);
            } else if (context instanceof List<?> list) {
                try {
                    int index = Integer.parseInt(pathElement);
                    if (index < 0 || index >= list.size()) {
                        if (failOutOfRange) {
                            throw new IllegalArgumentException(
                                "["
                                    + index
                                    + "] is out of bounds for array with length ["
                                    + list.size()
                                    + "] as part of path ["
                                    + path
                                    + "]"
                            );
                        } else {
                            return false;
                        }
                    }
                    context = list.get(index);
                } catch (NumberFormatException e) {
                    return false;
                }

            } else {
                return false;
            }
        }

        String leafKey = fieldPath.pathElements[fieldPath.pathElements.length - 1];
        if (context instanceof Map<?, ?> map) {
            return map.containsKey(leafKey);
        }
        if (context instanceof List<?> list) {
            try {
                int index = Integer.parseInt(leafKey);
                if (index >= 0 && index < list.size()) {
                    return true;
                } else {
                    if (failOutOfRange) {
                        throw new IllegalArgumentException(
                            "[" + index + "] is out of bounds for array with length [" + list.size() + "] as part of path [" + path + "]"
                        );
                    } else {
                        return false;
                    }
                }
            } catch (NumberFormatException e) {
                return false;
            }
        }
        return false;
    }

    /**
     * Removes the field identified by the provided path.
     * @param fieldPathTemplate Resolves to the path with dot-notation within the document
     * @throws IllegalArgumentException if the path is null, empty, invalid or if the field doesn't exist.
     */
    public void removeField(TemplateScript.Factory fieldPathTemplate) {
        removeField(renderTemplate(fieldPathTemplate));
    }

    /**
     * Removes the field identified by the provided path.
     * @param path the path of the field to be removed
     * @throws IllegalArgumentException if the path is null, empty, invalid or if the field doesn't exist.
     */
    public void removeField(String path) {
        FieldPath fieldPath = new FieldPath(path);
        Object context = fieldPath.initialContext;
        for (int i = 0; i < fieldPath.pathElements.length - 1; i++) {
            ResolveResult result = resolve(fieldPath.pathElements[i], path, context);
            if (result.wasSuccessful) {
                context = result.resolvedObject;
            } else {
                throw new IllegalArgumentException(result.errorMessage);
            }
        }

        String leafKey = fieldPath.pathElements[fieldPath.pathElements.length - 1];
        if (context instanceof Map<?, ?> map) {
            if (map.containsKey(leafKey)) {
                map.remove(leafKey);
                return;
            }
            throw new IllegalArgumentException("field [" + leafKey + "] not present as part of path [" + path + "]");
        }
        if (context instanceof List<?> list) {
            int index;
            try {
                index = Integer.parseInt(leafKey);
            } catch (NumberFormatException e) {
                throw new IllegalArgumentException(
                    "[" + leafKey + "] is not an integer, cannot be used as an index as part of path [" + path + "]",
                    e
                );
            }
            if (index < 0 || index >= list.size()) {
                throw new IllegalArgumentException(
                    "[" + index + "] is out of bounds for array with length [" + list.size() + "] as part of path [" + path + "]"
                );
            }
            list.remove(index);
            return;
        }

        if (context == null) {
            throw new IllegalArgumentException("cannot remove [" + leafKey + "] from null as part of path [" + path + "]");
        }
        throw new IllegalArgumentException(
            "cannot remove [" + leafKey + "] from object of type [" + context.getClass().getName() + "] as part of path [" + path + "]"
        );
    }

    private static ResolveResult resolve(String pathElement, String fullPath, Object context) {
        if (context == null) {
            return ResolveResult.error("cannot resolve [" + pathElement + "] from null as part of path [" + fullPath + "]");
        }
        if (context instanceof Map<?, ?> map) {
            if (map.containsKey(pathElement)) {
                return ResolveResult.success(map.get(pathElement));
            }
            return ResolveResult.error("field [" + pathElement + "] not present as part of path [" + fullPath + "]");
        }
        if (context instanceof List<?> list) {
            int index;
            try {
                index = Integer.parseInt(pathElement);
            } catch (NumberFormatException e) {
                return ResolveResult.error(
                    "[" + pathElement + "] is not an integer, cannot be used as an index as part of path [" + fullPath + "]"
                );
            }
            if (index < 0 || index >= list.size()) {
                return ResolveResult.error(
                    "[" + index + "] is out of bounds for array with length [" + list.size() + "] as part of path [" + fullPath + "]"
                );
            }
            return ResolveResult.success(list.get(index));
        }
        return ResolveResult.error(
            "cannot resolve ["
                + pathElement
                + "] from object of type ["
                + context.getClass().getName()
                + "] as part of path ["
                + fullPath
                + "]"
        );
    }

    /**
     * Appends the provided value to the provided path in the document.
     * Any non existing path element will be created.
     * If the path identifies a list, the value will be appended to the existing list.
     * If the path identifies a scalar, the scalar will be converted to a list and
     * the provided value will be added to the newly created list.
     * Supports multiple values too provided in forms of list, in that case all the values will be appended to the
     * existing (or newly created) list.
     * @param path The path within the document in dot-notation
     * @param value The value or values to append to the existing ones
     * @throws IllegalArgumentException if the path is null, empty or invalid.
     */
    public void appendFieldValue(String path, Object value) {
        appendFieldValue(path, value, true);
    }

    /**
     * Appends the provided value to the provided path in the document.
     * Any non existing path element will be created.
     * If the path identifies a list, the value will be appended to the existing list.
     * If the path identifies a scalar, the scalar will be converted to a list and
     * the provided value will be added to the newly created list.
     * Supports multiple values too provided in forms of list, in that case all the values will be appended to the
     * existing (or newly created) list.
     * @param path The path within the document in dot-notation
     * @param value The value or values to append to the existing ones
     * @param allowDuplicates When false, any values that already exist in the field will not be added
     * @throws IllegalArgumentException if the path is null, empty or invalid.
     */
    public void appendFieldValue(String path, Object value, boolean allowDuplicates) {
        setFieldValue(path, value, true, allowDuplicates);
    }

    /**
     * Appends the provided value to the provided path in the document.
     * Any non existing path element will be created.
     * If the path identifies a list, the value will be appended to the existing list.
     * If the path identifies a scalar, the scalar will be converted to a list and
     * the provided value will be added to the newly created list.
     * Supports multiple values too provided in forms of list, in that case all the values will be appended to the
     * existing (or newly created) list.
     * @param fieldPathTemplate Resolves to the path with dot-notation within the document
     * @param valueSource The value source that will produce the value or values to append to the existing ones
     * @throws IllegalArgumentException if the path is null, empty or invalid.
     */
    public void appendFieldValue(TemplateScript.Factory fieldPathTemplate, ValueSource valueSource) {
        Map<String, Object> model = createTemplateModel();
        appendFieldValue(fieldPathTemplate.newInstance(model).execute(), valueSource.copyAndResolve(model));
    }

    /**
     * Appends the provided value to the provided path in the document.
     * Any non existing path element will be created.
     * If the path identifies a list, the value will be appended to the existing list.
     * If the path identifies a scalar, the scalar will be converted to a list and
     * the provided value will be added to the newly created list.
     * Supports multiple values too provided in forms of list, in that case all the values will be appended to the
     * existing (or newly created) list.
     * @param fieldPathTemplate Resolves to the path with dot-notation within the document
     * @param valueSource The value source that will produce the value or values to append to the existing ones
     * @param allowDuplicates When false, any values that already exist in the field will not be added
     * @throws IllegalArgumentException if the path is null, empty or invalid.
     */
    public void appendFieldValue(TemplateScript.Factory fieldPathTemplate, ValueSource valueSource, boolean allowDuplicates) {
        Map<String, Object> model = createTemplateModel();
        appendFieldValue(fieldPathTemplate.newInstance(model).execute(), valueSource.copyAndResolve(model), allowDuplicates);
    }

    /**
     * Sets the provided value to the provided path in the document.
     * Any non existing path element will be created.
     * If the last item in the path is a list, the value will replace the existing list as a whole.
     * Use {@link #appendFieldValue(String, Object)} to append values to lists instead.
     * @param path The path within the document in dot-notation
     * @param value The value to put in for the path key
     * @throws IllegalArgumentException if the path is null, empty, invalid or if the value cannot be set to the
     * item identified by the provided path.
     */
    public void setFieldValue(String path, Object value) {
        setFieldValue(path, value, false);
    }

    /**
     * Sets the provided value to the provided path in the document.
     * Any non existing path element will be created. If the last element is a list,
     * the value will replace the existing list.
     * @param fieldPathTemplate Resolves to the path with dot-notation within the document
     * @param valueSource The value source that will produce the value to put in for the path key
     * @throws IllegalArgumentException if the path is null, empty, invalid or if the value cannot be set to the
     * item identified by the provided path.
     */
    public void setFieldValue(TemplateScript.Factory fieldPathTemplate, ValueSource valueSource) {
        Map<String, Object> model = createTemplateModel();
        setFieldValue(fieldPathTemplate.newInstance(model).execute(), valueSource.copyAndResolve(model), false);
    }

    /**
     * Sets the provided value to the provided path in the document.
     * Any non existing path element will be created. If the last element is a list,
     * the value will replace the existing list.
     * @param fieldPathTemplate Resolves to the path with dot-notation within the document
     * @param valueSource The value source that will produce the value to put in for the path key
     * @param ignoreEmptyValue The flag to determine whether to exit quietly when the value produced by TemplatedValue is null or empty
     * @throws IllegalArgumentException if the path is null, empty, invalid or if the value cannot be set to the
     * item identified by the provided path.
     */
    public void setFieldValue(TemplateScript.Factory fieldPathTemplate, ValueSource valueSource, boolean ignoreEmptyValue) {
        Map<String, Object> model = createTemplateModel();
        Object value = valueSource.copyAndResolve(model);
        if (ignoreEmptyValue && valueSource instanceof ValueSource.TemplatedValue) {
            if (value == null) {
                return;
            }
            String valueStr = (String) value;
            if (valueStr.isEmpty()) {
                return;
            }
        }

        setFieldValue(fieldPathTemplate.newInstance(model).execute(), value, false);
    }

    /**
     * Sets the provided value to the provided path in the document.
     * Any non existing path element will be created. If the last element is a list,
     * the value will replace the existing list.
     * @param fieldPathTemplate Resolves to the path with dot-notation within the document
     * @param value The value to put in for the path key
     * @param ignoreEmptyValue The flag to determine whether to exit quietly when the value produced by TemplatedValue is null or empty
     * @throws IllegalArgumentException if the path is null, empty, invalid or if the value cannot be set to the
     * item identified by the provided path.
     */
    public void setFieldValue(TemplateScript.Factory fieldPathTemplate, Object value, boolean ignoreEmptyValue) {
        Map<String, Object> model = createTemplateModel();
        if (ignoreEmptyValue) {
            if (value == null) {
                return;
            }
            if (value instanceof String string) {
                if (string.isEmpty()) {
                    return;
                }
            }
        }

        setFieldValue(fieldPathTemplate.newInstance(model).execute(), value, false);
    }

    private void setFieldValue(String path, Object value, boolean append) {
        setFieldValue(path, value, append, true);
    }

    private void setFieldValue(String path, Object value, boolean append, boolean allowDuplicates) {
        FieldPath fieldPath = new FieldPath(path);
        Object context = fieldPath.initialContext;
        for (int i = 0; i < fieldPath.pathElements.length - 1; i++) {
            String pathElement = fieldPath.pathElements[i];
            if (context == null) {
                throw new IllegalArgumentException("cannot resolve [" + pathElement + "] from null as part of path [" + path + "]");
            }
            if (context instanceof Map) {
                @SuppressWarnings("unchecked")
                Map<String, Object> map = (Map<String, Object>) context;
                if (map.containsKey(pathElement)) {
                    context = map.get(pathElement);
                } else {
                    HashMap<Object, Object> newMap = new HashMap<>();
                    map.put(pathElement, newMap);
                    context = newMap;
                }
            } else if (context instanceof List<?> list) {
                int index;
                try {
                    index = Integer.parseInt(pathElement);
                } catch (NumberFormatException e) {
                    throw new IllegalArgumentException(
                        "[" + pathElement + "] is not an integer, cannot be used as an index as part of path [" + path + "]",
                        e
                    );
                }
                if (index < 0 || index >= list.size()) {
                    throw new IllegalArgumentException(
                        "[" + index + "] is out of bounds for array with length [" + list.size() + "] as part of path [" + path + "]"
                    );
                }
                context = list.get(index);
            } else {
                throw new IllegalArgumentException(
                    "cannot resolve ["
                        + pathElement
                        + "] from object of type ["
                        + context.getClass().getName()
                        + "] as part of path ["
                        + path
                        + "]"
                );
            }
        }

        String leafKey = fieldPath.pathElements[fieldPath.pathElements.length - 1];
        if (context == null) {
            throw new IllegalArgumentException("cannot set [" + leafKey + "] with null parent as part of path [" + path + "]");
        }
        if (context instanceof Map) {
            @SuppressWarnings("unchecked")
            Map<String, Object> map = (Map<String, Object>) context;
            if (append) {
                if (map.containsKey(leafKey)) {
                    Object object = map.get(leafKey);
                    Object list = appendValues(object, value, allowDuplicates);
                    if (list != object) {
                        map.put(leafKey, list);
                    }
                } else {
                    List<Object> list = new ArrayList<>();
                    appendValues(list, value);
                    map.put(leafKey, list);
                }
                return;
            }
            map.put(leafKey, value);
        } else if (context instanceof List) {
            @SuppressWarnings("unchecked")
            List<Object> list = (List<Object>) context;
            int index;
            try {
                index = Integer.parseInt(leafKey);
            } catch (NumberFormatException e) {
                throw new IllegalArgumentException(
                    "[" + leafKey + "] is not an integer, cannot be used as an index as part of path [" + path + "]",
                    e
                );
            }
            if (index < 0 || index >= list.size()) {
                throw new IllegalArgumentException(
                    "[" + index + "] is out of bounds for array with length [" + list.size() + "] as part of path [" + path + "]"
                );
            }
            if (append) {
                Object object = list.get(index);
                Object newList = appendValues(object, value, allowDuplicates);
                if (newList != object) {
                    list.set(index, newList);
                }
                return;
            }
            list.set(index, value);
        } else {
            throw new IllegalArgumentException(
                "cannot set ["
                    + leafKey
                    + "] with parent object of type ["
                    + context.getClass().getName()
                    + "] as part of path ["
                    + path
                    + "]"
            );
        }
    }

    @SuppressWarnings("unchecked")
    private static Object appendValues(Object maybeList, Object value, boolean allowDuplicates) {
        List<Object> list;
        if (maybeList instanceof List) {
            // maybeList is already a list, we append the provided values to it
            list = (List<Object>) maybeList;
        } else {
            // maybeList is a scalar, we convert it to a list and append the provided values to it
            list = new ArrayList<>();
            list.add(maybeList);
        }
        if (allowDuplicates) {
            appendValues(list, value);
            return list;
        } else {
            // if no values were appended due to duplication, return the original object so the ingest document remains unmodified
            return appendValuesWithoutDuplicates(list, value) ? list : maybeList;
        }
    }

    private static void appendValues(List<Object> list, Object value) {
        if (value instanceof List<?> l) {
            list.addAll(l);
        } else {
            list.add(value);
        }
    }

    private static boolean appendValuesWithoutDuplicates(List<Object> list, Object value) {
        boolean valuesWereAppended = false;
        if (value instanceof List<?> valueList) {
            for (Object val : valueList) {
                if (list.contains(val) == false) {
                    list.add(val);
                    valuesWereAppended = true;
                }
            }
        } else {
            if (list.contains(value) == false) {
                list.add(value);
                valuesWereAppended = true;
            }
        }
        return valuesWereAppended;
    }

    private static <T> T cast(String path, Object object, Class<T> clazz) {
        if (object == null) {
            return null;
        }
        if (clazz.isInstance(object)) {
            return clazz.cast(object);
        }
        throw new IllegalArgumentException(
            "field [" + path + "] of type [" + object.getClass().getName() + "] cannot be cast to [" + clazz.getName() + "]"
        );
    }

    public String renderTemplate(TemplateScript.Factory template) {
        return template.newInstance(createTemplateModel()).execute();
    }

    private Map<String, Object> createTemplateModel() {
        return new LazyMap<>(() -> {
            Map<String, Object> model = new HashMap<>(ctxMap);
            model.put(SourceFieldMapper.NAME, ctxMap);
            // If there is a field in the source with the name '_ingest' it gets overwritten here,
            // if access to that field is required then it get accessed via '_source._ingest'
            model.put(INGEST_KEY, ingestMetadata);
            return model;
        });
    }

    /**
     * Get source and metadata map
     */
    public Map<String, Object> getSourceAndMetadata() {
        return ctxMap;
    }

    /**
     * Get the CtxMap
     */
    public CtxMap<?> getCtxMap() {
        return ctxMap;
    }

    /**
     * Get the strongly typed metadata
     */
    public org.elasticsearch.script.Metadata getMetadata() {
        return ctxMap.getMetadata();
    }

    /**
     * Get all source values in a Map
     */
    public Map<String, Object> getSource() {
        return ctxMap.getSource();
    }

    /**
     * Returns the available ingest metadata fields, by default only timestamp, but it is possible to set additional ones.
     * Use only for reading values, modify them instead using {@link #setFieldValue(String, Object)} and {@link #removeField(String)}
     */
    public Map<String, Object> getIngestMetadata() {
        return this.ingestMetadata;
    }

    @SuppressWarnings("unchecked")
    public static <K, V> Map<K, V> deepCopyMap(Map<K, V> source) {
        return (Map<K, V>) deepCopy(source);
    }

    public static Object deepCopy(Object value) {
        if (value instanceof Map<?, ?> mapValue) {
            Map<Object, Object> copy = Maps.newMapWithExpectedSize(mapValue.size());
            for (Map.Entry<?, ?> entry : mapValue.entrySet()) {
                copy.put(entry.getKey(), deepCopy(entry.getValue()));
            }
            // TODO(stu): should this check for IngestCtxMap in addition to Map?
            return copy;
        } else if (value instanceof List<?> listValue) {
            List<Object> copy = new ArrayList<>(listValue.size());
            for (Object itemValue : listValue) {
                copy.add(deepCopy(itemValue));
            }
            return copy;
        } else if (value instanceof Set<?> setValue) {
            Set<Object> copy = Sets.newHashSetWithExpectedSize(setValue.size());
            for (Object itemValue : setValue) {
                copy.add(deepCopy(itemValue));
            }
            return copy;
        } else if (value instanceof byte[] bytes) {
            return Arrays.copyOf(bytes, bytes.length);
        } else if (value instanceof double[][] doubles) {
            double[][] result = new double[doubles.length][];
            for (int i = 0; i < doubles.length; i++) {
                result[i] = Arrays.copyOf(doubles[i], doubles[i].length);
            }
            return result;
        } else if (value instanceof double[] doubles) {
            return Arrays.copyOf(doubles, doubles.length);
        } else if (value == null
            || value instanceof String
            || value instanceof Integer
            || value instanceof Long
            || value instanceof Float
            || value instanceof Double
            || value instanceof Boolean
            || value instanceof ZonedDateTime) {
                return value;
            } else if (value instanceof Date date) {
                return date.clone();
            } else {
                throw new IllegalArgumentException("unexpected value type [" + value.getClass() + "]");
            }
    }

    public static Set<String> getAllFields(Map<String, Object> input) {
        return getAllFields(input, "");
    }

    @SuppressWarnings("unchecked")
    private static Set<String> getAllFields(Map<String, Object> input, String prefix) {
        Set<String> allFields = Sets.newHashSet();

        input.forEach((k, v) -> {
            allFields.add(prefix + k);

            if (v instanceof Map<?, ?> mapValue) {
                allFields.addAll(getAllFields((Map<String, Object>) mapValue, prefix + k + "."));
            }
        });

        return allFields;
    }

    /**
     * Executes the given pipeline with for this document unless the pipeline has already been executed
     * for this document.
     *
     * @param pipeline the pipeline to execute
     * @param handler handles the result or failure
     */
    public void executePipeline(Pipeline pipeline, BiConsumer<IngestDocument, Exception> handler) {
        if (executedPipelines.add(pipeline.getId())) {
            Object previousPipeline = ingestMetadata.put("pipeline", pipeline.getId());
            pipeline.execute(this, (result, e) -> {
                executedPipelines.remove(pipeline.getId());
                if (previousPipeline != null) {
                    ingestMetadata.put("pipeline", previousPipeline);
                } else {
                    ingestMetadata.remove("pipeline");
                }
                handler.accept(result, e);
            });
        } else {
            handler.accept(null, new IllegalStateException(PIPELINE_CYCLE_ERROR_MESSAGE + pipeline.getId()));
        }
    }

    /**
     * @return a pipeline stack; all pipelines that are in execution by this document in reverse order
     */
    List<String> getPipelineStack() {
        List<String> pipelineStack = new ArrayList<>(executedPipelines);
        Collections.reverse(pipelineStack);
        return pipelineStack;
    }

    /**
     * @return Whether a self referencing check should be performed
     */
    public boolean doNoSelfReferencesCheck() {
        return doNoSelfReferencesCheck;
    }

    /**
     * Whether the ingest framework should perform a self referencing check after this ingest document
     * has been processed by all pipelines. Doing this check adds an extra tax to ingest and should
     * only be performed when really needed. Only if a processor is executed that could add self referencing
     * maps or lists then this check must be performed. Most processors will not be able to do this, hence
     * the default is <code>false</code>.
     *
     * @param doNoSelfReferencesCheck Whether a self referencing check should be performed
     */
    public void doNoSelfReferencesCheck(boolean doNoSelfReferencesCheck) {
        this.doNoSelfReferencesCheck = doNoSelfReferencesCheck;
    }

    @Override
    public boolean equals(Object obj) {
        if (obj == this) {
            return true;
        }
        if (obj == null || getClass() != obj.getClass()) {
            return false;
        }

        IngestDocument other = (IngestDocument) obj;
        return Objects.equals(ctxMap, other.ctxMap) && Objects.equals(ingestMetadata, other.ingestMetadata);
    }

    @Override
    public int hashCode() {
        return Objects.hash(ctxMap, ingestMetadata);
    }

    @Override
    public String toString() {
        return "IngestDocument{" + " sourceAndMetadata=" + ctxMap + ", ingestMetadata=" + ingestMetadata + '}';
    }

    public enum Metadata {
        INDEX(IndexFieldMapper.NAME),
        TYPE("_type"),
        ID(IdFieldMapper.NAME),
        ROUTING(RoutingFieldMapper.NAME),
        VERSION(VersionFieldMapper.NAME),
        VERSION_TYPE("_version_type"),
        IF_SEQ_NO("_if_seq_no"),
        IF_PRIMARY_TERM("_if_primary_term"),
        DYNAMIC_TEMPLATES("_dynamic_templates");

        private static final Set<String> METADATA_NAMES = Arrays.stream(Metadata.values())
            .map(metadata -> metadata.fieldName)
            .collect(Collectors.toSet());

        private final String fieldName;

        Metadata(String fieldName) {
            this.fieldName = fieldName;
        }

        public static boolean isMetadata(String field) {
            return METADATA_NAMES.contains(field);
        }

        public String getFieldName() {
            return fieldName;
        }
    }

    private class FieldPath {

        private final String[] pathElements;
        private final Object initialContext;

        private FieldPath(String path) {
            if (Strings.isEmpty(path)) {
                throw new IllegalArgumentException("path cannot be null nor empty");
            }
            String newPath;
            if (path.startsWith(INGEST_KEY_PREFIX)) {
                initialContext = ingestMetadata;
                newPath = path.substring(INGEST_KEY_PREFIX.length(), path.length());
            } else {
                initialContext = ctxMap;
                if (path.startsWith(SOURCE_PREFIX)) {
                    newPath = path.substring(SOURCE_PREFIX.length(), path.length());
                } else {
                    newPath = path;
                }
            }
            this.pathElements = newPath.split("\\.");
            if (pathElements.length == 1 && pathElements[0].isEmpty()) {
                throw new IllegalArgumentException("path [" + path + "] is not valid");
            }
        }

    }

    private static class ResolveResult {
        boolean wasSuccessful;
        String errorMessage;
        Object resolvedObject;

        static ResolveResult success(Object resolvedObject) {
            ResolveResult result = new ResolveResult();
            result.wasSuccessful = true;
            result.resolvedObject = resolvedObject;
            return result;
        }

        static ResolveResult error(String errorMessage) {
            ResolveResult result = new ResolveResult();
            result.wasSuccessful = false;
            result.errorMessage = errorMessage;
            return result;

        }
    }
}
