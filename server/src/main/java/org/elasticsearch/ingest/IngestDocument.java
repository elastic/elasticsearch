/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.ingest;

import org.elasticsearch.common.Strings;
import org.elasticsearch.common.util.CollectionUtils;
import org.elasticsearch.common.util.Maps;
import org.elasticsearch.common.util.concurrent.ConcurrentCollections;
import org.elasticsearch.common.util.set.Sets;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.core.UpdateForV10;
import org.elasticsearch.index.VersionType;
import org.elasticsearch.index.mapper.IdFieldMapper;
import org.elasticsearch.index.mapper.IndexFieldMapper;
import org.elasticsearch.index.mapper.RoutingFieldMapper;
import org.elasticsearch.index.mapper.SourceFieldMapper;
import org.elasticsearch.index.mapper.VersionFieldMapper;
import org.elasticsearch.script.CtxMap;
import org.elasticsearch.script.ScriptService;
import org.elasticsearch.script.TemplateScript;

import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Base64;
import java.util.Collection;
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
    public static final String SOURCE_KEY = SourceFieldMapper.NAME; // "_source"
    private static final String INGEST_KEY_PREFIX = INGEST_KEY + ".";
    private static final String SOURCE_PREFIX = SOURCE_KEY + ".";

    private static final String PIPELINE_CYCLE_ERROR_MESSAGE = "Cycle detected for pipeline: ";
    static final String TIMESTAMP = "timestamp";
    // This is the maximum number of nested pipelines that can be within a pipeline. If there are more, we bail out with an error
    public static final int MAX_PIPELINES = Integer.parseInt(System.getProperty("es.ingest.max_pipelines", "100"));

    // a 'not found' sentinel value for use in getOrDefault calls in order to avoid containsKey-and-then-get
    private static final Object NOT_FOUND = new Object();

    private final IngestCtxMap ctxMap;
    private final Map<String, Object> ingestMetadata;

    /**
     * Shallowly read-only, very limited, map-like view of the ctxMap and ingestMetadata,
     * for providing as a model to TemplateScript and ValueSource instances. This avoids the cost of
     * constructing a purpose-built map on each template evaluation.
     */
    private final DelegatingMapView templateModel;

    // Contains all pipelines that have been executed for this document
    private final Set<String> executedPipelines = new LinkedHashSet<>();

    /**
     * An ordered set of the values of the _index that have been used for this document.
     * <p>
     * IMPORTANT: This is only updated after a top-level pipeline has run (see {@code IngestService#executePipelines(...)}).
     * <p>
     * For example, if a processor changes the _index for a document from 'foo' to 'bar',
     * and then another processor changes the value back to 'foo', then the overall effect
     * of the pipeline was that the _index value did not change and so only 'foo' would appear
     * in the index history.
     */
    private final Set<String> indexHistory = new LinkedHashSet<>();

    private boolean doNoSelfReferencesCheck = false;
    private boolean reroute = false;
    private boolean terminate = false;

    // reusing the value list in order to avoid allocations, as in most cases we won't need it, or there will be only one value
    private static final ThreadLocal<List<?>> threadLocalValues = ThreadLocal.withInitial(ArrayList::new);

    public IngestDocument(String index, String id, long version, String routing, VersionType versionType, Map<String, Object> source) {
        this.ctxMap = new IngestCtxMap(index, id, version, routing, versionType, ZonedDateTime.now(ZoneOffset.UTC), source);
        this.ingestMetadata = new HashMap<>();
        this.ingestMetadata.put(TIMESTAMP, ctxMap.getMetadata().getNow());
        this.templateModel = initializeTemplateModel();

        // initialize the index history by putting the current index into it
        this.indexHistory.add(index);
    }

    // note: these rest of these constructors deal with the data-centric view of the IngestDocument, not the execution-centric view.
    // For example, the copy constructor doesn't populate the `indexHistory` (as well as some other fields),
    // because those fields are execution-centric.

    /**
     * Copy constructor that creates a new {@link IngestDocument} which has exactly the same properties as the one provided.
     *
     * @throws IllegalArgumentException if the passed-in ingest document references itself
     */
    public IngestDocument(IngestDocument other) {
        this(
            new IngestCtxMap(deepCopyMap(ensureNoSelfReferences(other.ctxMap.getSource())), other.ctxMap.getMetadata().clone()),
            deepCopyMap(other.ingestMetadata)
        );
        /*
         * The executedPipelines field is clearly execution-centric rather than data centric. Despite what the comment above says, we're
         * copying it here anyway. THe reason is that this constructor is only called from two non-test locations, and both of those
         * involve the simulate pipeline logic. The simulate pipeline logic needs this information. Rather than making the code more
         * complicated, we're just copying this over here since it does no harm.
         */
        this.executedPipelines.addAll(other.executedPipelines);
    }

    /**
     * Internal helper utility method to get around the issue that a {@code this(...) } constructor call must be the first statement
     * in a constructor. This is only for use in the {@link IngestDocument#IngestDocument(IngestDocument)} copy constructor, it's not a
     * general purpose method.
     */
    private static Map<String, Object> ensureNoSelfReferences(Map<String, Object> source) {
        CollectionUtils.ensureNoSelfReferences(source, null);
        return source;
    }

    /**
     * Constructor to create an IngestDocument from its constituent maps. The maps are shallow copied.
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
        this.ctxMap = new IngestCtxMap(source, new IngestDocMetadata(metadata, IngestCtxMap.getTimestamp(ingestMetadata)));
        this.ingestMetadata = new HashMap<>(ingestMetadata);
        this.templateModel = initializeTemplateModel();
    }

    /**
     * Constructor to create an IngestDocument from its constituent maps.
     */
    IngestDocument(IngestCtxMap ctxMap, Map<String, Object> ingestMetadata) {
        this.ctxMap = Objects.requireNonNull(ctxMap);
        this.ingestMetadata = Objects.requireNonNull(ingestMetadata);
        this.templateModel = initializeTemplateModel();
    }

    private DelegatingMapView initializeTemplateModel() {
        return new DelegatingMapView(ctxMap, Map.of(SOURCE_KEY, ctxMap, INGEST_KEY, ingestMetadata));
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
        final DotNotationFieldPath fieldPath = DotNotationFieldPath.of(path);
        Object context = fieldPath.initialContext(this);
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
            throw new IllegalArgumentException(Errors.notStringOrByteArray(path, object));
        }
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
        final DotNotationFieldPath fieldPath = DotNotationFieldPath.of(path);
        Object context = fieldPath.initialContext(this);
        for (int i = 0; i < fieldPath.pathElements.length - 1; i++) {
            String pathElement = fieldPath.pathElements[i];
            if (context == null) {
                return false;
            } else if (context instanceof Map<?, ?> map) {
                context = map.get(pathElement);
            } else if (context instanceof List<?> list) {
                int index;
                try {
                    index = Integer.parseInt(pathElement);
                } catch (NumberFormatException e) {
                    return false;
                }
                if (index < 0 || index >= list.size()) {
                    if (failOutOfRange) {
                        throw new IllegalArgumentException(Errors.outOfBounds(path, index, list.size()));
                    } else {
                        return false;
                    }
                } else {
                    context = list.get(index);
                }
            } else {
                return false;
            }
        }

        String leafKey = fieldPath.pathElements[fieldPath.pathElements.length - 1];
        return hasDirectChildField(context, leafKey, path, failOutOfRange);
    }

    private boolean hasDirectChildField(Object parent, String fieldName, String path, boolean failOutOfRange) {
        if (parent == null) {
            return false;
        } else if (parent instanceof Map<?, ?> map) {
            return map.containsKey(fieldName);
        } else if (parent instanceof List<?> list) {
            try {
                int index = Integer.parseInt(fieldName);
                if (index >= 0 && index < list.size()) {
                    return true;
                } else {
                    if (failOutOfRange) {
                        throw new IllegalArgumentException(Errors.outOfBounds(path, index, list.size()));
                    } else {
                        return false;
                    }
                }
            } catch (NumberFormatException e) {
                return false;
            }
        } else {
            return false;
        }
    }

    /**
     * Removes the field identified by the provided path.
     * @param path the path of the field to be removed
     * @throws IllegalArgumentException if the path is null, empty, invalid or if the field doesn't exist.
     */
    public void removeField(String path) {
        final DotNotationFieldPath fieldPath = DotNotationFieldPath.of(path);
        Object context = fieldPath.initialContext(this);
        for (int i = 0; i < fieldPath.pathElements.length - 1; i++) {
            ResolveResult result = resolve(fieldPath.pathElements[i], path, context);
            if (result.wasSuccessful) {
                context = result.resolvedObject;
            } else {
                throw new IllegalArgumentException(result.errorMessage);
            }
        }

        String leafKey = fieldPath.pathElements[fieldPath.pathElements.length - 1];
        removeDirectChildField(context, leafKey, path);
    }

    private void removeDirectChildField(Object parent, String fieldName, String fullPath) {
        if (parent == null) {
            throw new IllegalArgumentException(Errors.cannotRemove(fullPath, fieldName, null));
        } else if (parent instanceof Map<?, ?> map) {
            if (map.containsKey(fieldName)) {
                map.remove(fieldName);
            } else {
                throw new IllegalArgumentException(Errors.notPresent(fullPath, fieldName));
            }
        } else if (parent instanceof List<?> list) {
            int index;
            try {
                index = Integer.parseInt(fieldName);
            } catch (NumberFormatException e) {
                throw new IllegalArgumentException(Errors.notInteger(fullPath, fieldName), e);
            }
            if (index < 0 || index >= list.size()) {
                throw new IllegalArgumentException(Errors.outOfBounds(fullPath, index, list.size()));
            } else {
                list.remove(index);
            }
        } else {
            throw new IllegalArgumentException(Errors.cannotRemove(fullPath, fieldName, parent));
        }
    }

    private static ResolveResult resolve(String pathElement, String fullPath, Object context) {
        if (context == null) {
            return ResolveResult.error(Errors.cannotResolve(fullPath, pathElement, null));
        } else if (context instanceof Map<?, ?>) {
            @SuppressWarnings("unchecked")
            Map<String, Object> map = (Map<String, Object>) context;
            Object object = map.getOrDefault(pathElement, NOT_FOUND); // getOrDefault is faster than containsKey + get
            if (object == NOT_FOUND) {
                return ResolveResult.error(Errors.notPresent(fullPath, pathElement));
            } else {
                return ResolveResult.success(object);
            }
        } else if (context instanceof List<?> list) {
            int index;
            try {
                index = Integer.parseInt(pathElement);
            } catch (NumberFormatException e) {
                return ResolveResult.error(Errors.notInteger(fullPath, pathElement));
            }
            if (index < 0 || index >= list.size()) {
                return ResolveResult.error(Errors.outOfBounds(fullPath, index, list.size()));
            } else {
                return ResolveResult.success(list.get(index));
            }
        } else {
            return ResolveResult.error(Errors.cannotResolve(fullPath, pathElement, context));
        }
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
     * @param path The path within the document in dot-notation
     * @param valueSource The value source that will produce the value or values to append to the existing ones
     * @param allowDuplicates When false, any values that already exist in the field will not be added
     * @throws IllegalArgumentException if the path is null, empty or invalid.
     */
    public void appendFieldValue(String path, ValueSource valueSource, boolean allowDuplicates) {
        appendFieldValue(path, valueSource.copyAndResolve(templateModel), allowDuplicates);
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
        setFieldValue(path, value, false, true);
    }

    /**
     * Sets the provided value to the provided path in the document.
     * Any non existing path element will be created. If the last element is a list,
     * the value will replace the existing list.
     * @param path The path within the document in dot-notation
     * @param valueSource The value source that will produce the value to put in for the path key
     * @throws IllegalArgumentException if the path is null, empty, invalid or if the value cannot be set to the
     * item identified by the provided path.
     */
    public void setFieldValue(String path, ValueSource valueSource) {
        setFieldValue(path, valueSource.copyAndResolve(templateModel));
    }

    /**
     * Sets the provided value to the provided path in the document.
     * Any non existing path element will be created. If the last element is a list,
     * the value will replace the existing list.
     * @param path The path within the document in dot-notation
     * @param valueSource The value source that will produce the value to put in for the path key
     * @param ignoreEmptyValue The flag to determine whether to exit quietly when the value produced by TemplatedValue is null or empty
     * @throws IllegalArgumentException if the path is null, empty, invalid or if the value cannot be set to the
     * item identified by the provided path.
     */
    public void setFieldValue(String path, ValueSource valueSource, boolean ignoreEmptyValue) {
        Object value = valueSource.copyAndResolve(templateModel);
        if (ignoreEmptyValue && valueSource instanceof ValueSource.TemplatedValue) {
            if (value == null) {
                return;
            }
            String valueStr = (String) value;
            if (valueStr.isEmpty()) {
                return;
            }
        }
        setFieldValue(path, value);
    }

    /**
     * Sets the provided value to the provided path in the document.
     * Any non existing path element will be created. If the last element is a list,
     * the value will replace the existing list.
     * @param path The path within the document in dot-notation
     * @param value The value to put in for the path key
     * @param ignoreEmptyValue The flag to determine whether to exit quietly when the value produced by TemplatedValue is null or empty
     * @throws IllegalArgumentException if the path is null, empty, invalid or if the value cannot be set to the
     * item identified by the provided path.
     */
    public void setFieldValue(String path, Object value, boolean ignoreEmptyValue) {
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
        setFieldValue(path, value);
    }

    private void setFieldValue(String path, Object value, boolean append, boolean allowDuplicates) {
        final DotNotationFieldPath fieldPath = DotNotationFieldPath.of(path);
        Object context = fieldPath.initialContext(this);
        for (int i = 0; i < fieldPath.pathElements.length - 1; i++) {
            String pathElement = fieldPath.pathElements[i];
            if (context == null) {
                throw new IllegalArgumentException(Errors.cannotResolve(path, pathElement, null));
            } else if (context instanceof Map<?, ?>) {
                @SuppressWarnings("unchecked")
                Map<String, Object> map = (Map<String, Object>) context;
                Object object = map.getOrDefault(pathElement, NOT_FOUND); // getOrDefault is faster than containsKey + get
                if (object == NOT_FOUND) {
                    Map<Object, Object> newMap = new HashMap<>();
                    map.put(pathElement, newMap);
                    context = newMap;
                } else {
                    context = object;
                }
            } else if (context instanceof List<?> list) {
                int index;
                try {
                    index = Integer.parseInt(pathElement);
                } catch (NumberFormatException e) {
                    throw new IllegalArgumentException(Errors.notInteger(path, pathElement), e);
                }
                if (index < 0 || index >= list.size()) {
                    throw new IllegalArgumentException(Errors.outOfBounds(path, index, list.size()));
                } else {
                    context = list.get(index);
                }
            } else {
                throw new IllegalArgumentException(Errors.cannotResolve(path, pathElement, context));
            }
        }

        String leafKey = fieldPath.pathElements[fieldPath.pathElements.length - 1];
        setDirectChildFieldValue(context, leafKey, value, path, append, allowDuplicates);
    }

    private void setDirectChildFieldValue(
        Object parent,
        String fieldName,
        Object value,
        String fullPath,
        boolean append,
        boolean allowDuplicates
    ) {
        if (parent == null) {
            throw new IllegalArgumentException(Errors.cannotSet(fullPath, fieldName, null));
        } else if (parent instanceof Map<?, ?>) {
            @SuppressWarnings("unchecked")
            Map<String, Object> map = (Map<String, Object>) parent;
            if (append) {
                Object object = map.getOrDefault(fieldName, NOT_FOUND); // getOrDefault is faster than containsKey + get
                if (object == NOT_FOUND) {
                    List<Object> list = new ArrayList<>();
                    appendValues(list, value);
                    map.put(fieldName, list);
                } else {
                    Object list = appendValues(object, value, allowDuplicates);
                    if (list != object) {
                        map.put(fieldName, list);
                    }
                }
                return;
            }
            map.put(fieldName, value);
        } else if (parent instanceof List<?>) {
            @SuppressWarnings("unchecked")
            List<Object> list = (List<Object>) parent;
            int index;
            try {
                index = Integer.parseInt(fieldName);
            } catch (NumberFormatException e) {
                throw new IllegalArgumentException(Errors.notInteger(fullPath, fieldName), e);
            }
            if (index < 0 || index >= list.size()) {
                throw new IllegalArgumentException(Errors.outOfBounds(fullPath, index, list.size()));
            } else {
                if (append) {
                    Object object = list.get(index);
                    Object newList = appendValues(object, value, allowDuplicates);
                    if (newList != object) {
                        list.set(index, newList);
                    }
                    return;
                }
                list.set(index, value);
            }
        } else {
            throw new IllegalArgumentException(Errors.cannotSet(fullPath, fieldName, parent));
        }
    }

    private void appendDirectChildFieldValue(Object parent, String fieldName, Object value) {
        setDirectChildFieldValue(parent, fieldName, value, fieldName, true, true);
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
        throw new IllegalArgumentException(Errors.cannotCast(path, object, clazz));
    }

    /**
     * Renders a template into a string. This allows field access via both literal fields like {@code "foo.bar.baz"} and dynamic fields
     * like {@code "{{other_field}}"} (that is, look up the value of the 'other_field' in the document and then use the resulting string as
     * the field to operate on).
     * <p>
     * See {@link ConfigurationUtils#compileTemplate(String, String, String, String, ScriptService)} and associated methods, which
     * create these {@link TemplateScript.Factory} instances.
     * <p>
     * Note: for clarity and efficiency reasons, it is advisable to invoke this method outside IngestDocument itself -- fields should be
     * rendered by a caller (once), and then passed to an ingest document repeatedly. There are enough methods on IngestDocument that
     * operate on String paths already, we don't want to mirror all of them with twin methods that accept a template.
     *
     * @param template the template or literal string to evaluate
     * @return a literal string field path
     */
    public String renderTemplate(TemplateScript.Factory template) {
        return template.newInstance(templateModel).execute();
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
        // shortcut if the pipeline is empty
        if (pipeline.getProcessors().isEmpty()) {
            handler.accept(this, null);
            return;
        }

        if (executedPipelines.size() >= MAX_PIPELINES) {
            handler.accept(
                null,
                new GraphStructureException("Too many nested pipelines. Cannot have more than " + MAX_PIPELINES + " nested pipelines")
            );
        } else if (executedPipelines.add(pipeline.getId())) {
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
            handler.accept(null, new GraphStructureException(PIPELINE_CYCLE_ERROR_MESSAGE + pipeline.getId()));
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
     * Adds an index to the index history for this document, returning true if the index
     * was added to the index history (i.e. if it wasn't already in the index history).
     *
     * @param index the index to potentially add to the index history
     * @return true if the index history did not already contain the index in question
     */
    public boolean updateIndexHistory(String index) {
        return indexHistory.add(index);
    }

    /**
     * @return an unmodifiable view of the document's index history
     */
    public Set<String> getIndexHistory() {
        return Collections.unmodifiableSet(indexHistory);
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
    public String toString() {
        return "IngestDocument{" + " sourceAndMetadata=" + ctxMap + ", ingestMetadata=" + ingestMetadata + '}';
    }

    public void reroute(String destIndex) {
        getMetadata().setIndex(destIndex);
        reroute = true;
    }

    /**
     * The document is redirected to another target.
     * This implies that we'll skip the current pipeline and invoke the default pipeline of the new target
     *
     * @return whether the document is redirected to another target
     */
    boolean isReroute() {
        return reroute;
    }

    /**
     * Set the {@link #reroute} flag to false so that subsequent calls to {@link #isReroute()} will return false until/unless
     * {@link #reroute(String)} is called.
     */
    void resetReroute() {
        reroute = false;
    }

    /**
     * Sets the terminate flag to true, to indicate that no further processors in the current pipeline should be run for this document.
     */
    public void terminate() {
        terminate = true;
    }

    /**
     * Returns whether the {@link #terminate()} flag was set.
     */
    boolean isTerminate() {
        return terminate;
    }

    /**
     * Resets the {@link #terminate()} flag.
     */
    void resetTerminate() {
        terminate = false;
    }

    // Unconditionally deprecate the _type field once V7 BWC support is removed
    @UpdateForV10(owner = UpdateForV10.Owner.DATA_MANAGEMENT)
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

    private static class FieldPath {

        private static final int MAX_SIZE = 512;
        private static final Map<String, FieldPath> CACHE = ConcurrentCollections.newConcurrentMapWithAggressiveConcurrency();

        private final boolean useIngestContext;
        private final String trimmedPath;

        // you shouldn't call this directly, use the FieldPath.of method above instead!
        protected FieldPath(String path) {
            String newPath;
            if (path.startsWith(INGEST_KEY_PREFIX)) {
                useIngestContext = true;
                newPath = path.substring(INGEST_KEY_PREFIX.length());
            } else {
                useIngestContext = false;
                if (path.startsWith(SOURCE_PREFIX)) {
                    newPath = path.substring(SOURCE_PREFIX.length());
                } else {
                    newPath = path;
                }
            }
            this.trimmedPath = newPath;
        }

        public Object initialContext(IngestDocument document) {
            return useIngestContext ? document.getIngestMetadata() : document.getCtxMap();
        }

        public String getTrimmedPath() {
            return trimmedPath;
        }

        static FieldPath of(String path) {
            if (Strings.isEmpty(path)) {
                throw new IllegalArgumentException("path cannot be null nor empty");
            }
            FieldPath res = CACHE.get(path);
            if (res != null) {
                return res;
            }
            res = new FieldPath(path);
            if (CACHE.size() > MAX_SIZE) {
                CACHE.clear();
            }
            CACHE.put(path, res);
            return res;
        }
    }

    private static final class DotNotationFieldPath extends FieldPath {

        private static final int MAX_SIZE = 512;
        private static final Map<String, DotNotationFieldPath> CACHE = ConcurrentCollections.newConcurrentMapWithAggressiveConcurrency();

        // constructing a new FieldPath requires that we parse a String (e.g. "foo.bar.baz") into an array
        // of path elements (e.g. ["foo", "bar", "baz"]). Calling String#split results in the allocation
        // of an ArrayList to hold the results, then a new String is created for each path element, and
        // then finally a String[] is allocated to hold the actual result -- in addition to all that, we
        // do some processing ourselves on the path and path elements to validate and prepare them.
        // the above CACHE and the below 'FieldPath.of' method allow us to almost always avoid this work.

        static DotNotationFieldPath of(String path) {
            if (Strings.isEmpty(path)) {
                throw new IllegalArgumentException("path cannot be null nor empty");
            }
            DotNotationFieldPath res = CACHE.get(path);
            if (res != null) {
                return res;
            }
            res = new DotNotationFieldPath(path);
            if (CACHE.size() > MAX_SIZE) {
                CACHE.clear();
            }
            CACHE.put(path, res);
            return res;
        }

        private final String[] pathElements;

        // you shouldn't call this directly, use the FieldPath.of method above instead!
        private DotNotationFieldPath(String path) {
            super(path);
            this.pathElements = getTrimmedPath().split("\\.");
            if (pathElements.length == 1 && pathElements[0].isEmpty()) {
                throw new IllegalArgumentException("path [" + path + "] is not valid");
            }
        }
    }

    private record ResolveResult(boolean wasSuccessful, Object resolvedObject, String errorMessage) {
        static ResolveResult success(Object resolvedObject) {
            return new ResolveResult(true, resolvedObject, null);
        }

        static ResolveResult error(String errorMessage) {
            return new ResolveResult(false, null, errorMessage);
        }
    }

    /**
     * Provides a shallowly read-only, very limited, map-like view of two maps. The only methods that are implemented are
     * {@link Map#get(Object)} and {@link Map#containsKey(Object)}, everything else throws UnsupportedOperationException.
     * <p>
     * The overrides map has higher priority than the primary map -- values in that map under some key will take priority over values
     * in the primary map under the same key.
     *
     * @param primary the primary map
     * @param overrides the overrides map
     */
    private record DelegatingMapView(Map<String, Object> primary, Map<String, Object> overrides) implements Map<String, Object> {

        @Override
        public boolean containsKey(Object key) {
            // most normal uses of this in practice will end up passing in keys that match the primary, rather than the overrides,
            // in which case we can shortcut by checking the primary first
            return primary.containsKey(key) || overrides.containsKey(key);
        }

        @Override
        public Object get(Object key) {
            // null values in the overrides map are treated as *key not present*, so we don't have to do a containsKey check here --
            // if the overrides map returns null we can simply delegate to the primary
            Object result = overrides.get(key);
            return result != null ? result : primary.get(key);
        }

        @Override
        public int size() {
            throw new UnsupportedOperationException();
        }

        @Override
        public boolean isEmpty() {
            throw new UnsupportedOperationException();
        }

        @Override
        public boolean containsValue(Object value) {
            throw new UnsupportedOperationException();
        }

        @Override
        public Object put(String key, Object value) {
            throw new UnsupportedOperationException();
        }

        @Override
        public Object remove(Object key) {
            throw new UnsupportedOperationException();
        }

        @Override
        public void putAll(Map<? extends String, ?> m) {
            throw new UnsupportedOperationException();
        }

        @Override
        public void clear() {
            throw new UnsupportedOperationException();
        }

        @Override
        public Set<String> keySet() {
            throw new UnsupportedOperationException();
        }

        @Override
        public Collection<Object> values() {
            throw new UnsupportedOperationException();
        }

        @Override
        public Set<Entry<String, Object>> entrySet() {
            throw new UnsupportedOperationException();
        }
    }

    private static final class Errors {
        private Errors() {
            // utility class
        }

        private static String cannotCast(String path, Object value, Class<?> clazz) {
            return "field [" + path + "] of type [" + value.getClass().getName() + "] cannot be cast to [" + clazz.getName() + "]";
        }

        private static String cannotRemove(String path, String key, Object value) {
            if (value == null) {
                return "cannot remove [" + key + "] from null as part of path [" + path + "]";
            } else {
                final String type = value.getClass().getName();
                return "cannot remove [" + key + "] from object of type [" + type + "] as part of path [" + path + "]";
            }
        }

        private static String cannotResolve(String path, String key, Object value) {
            if (value == null) {
                return "cannot resolve [" + key + "] from null as part of path [" + path + "]";
            } else {
                final String type = value.getClass().getName();
                return "cannot resolve [" + key + "] from object of type [" + type + "] as part of path [" + path + "]";
            }
        }

        private static String cannotSet(String path, String key, Object value) {
            if (value == null) {
                return "cannot set [" + key + "] with null parent as part of path [" + path + "]";
            } else {
                final String type = value.getClass().getName();
                return "cannot set [" + key + "] with parent object of type [" + type + "] as part of path [" + path + "]";
            }
        }

        private static String outOfBounds(String path, int index, int length) {
            return "[" + index + "] is out of bounds for array with length [" + length + "] as part of path [" + path + "]";
        }

        private static String notInteger(String path, String key) {
            return "[" + key + "] is not an integer, cannot be used as an index as part of path [" + path + "]";
        }

        private static String notPresent(String path, String key) {
            return "field [" + key + "] not present as part of path [" + path + "]";
        }

        private static String notStringOrByteArray(String path, Object value) {
            return "Content field [" + path + "] of unknown type [" + value.getClass().getName() + "], must be string or byte array";
        }
    }

    /*=========================================== APIs for dotted fields =====================================================
     * As opposed to the above APIs, these APIs are designed to work with dotted fields, meaning
     * that the field path is not resolved assuming dot notation, but assuming that dots can either represent nested
     * fields or be part of the field name itself.
     =======================================================================================================================*/

    /**
     * Returns whether the document has a top level field with the exact provided field name, whether it contains dots or not.
     * @param fieldName the field name to check for
     * @return whether the document has a top level field with the provided name
     */
    public boolean hasTopLevelField(String fieldName) {
        FieldPath fieldPath = FieldPath.of(fieldName);
        Object root = fieldPath.initialContext(this);
        // since we only look in top level, it's irrelevant for lists, so we can pass false for the last argument
        return hasDirectChildField(root, fieldPath.getTrimmedPath(), fieldPath.getTrimmedPath(), false);
    }

    /**
     * Returns the value of the top level field with the exact provided field name, whether it contains dots or not. If the field does
     * not exist, returns {@code null} and does not throw an exception.
     * @param fieldName the field name to get the value of
     * @return the value of the top level field with the provided name or {@code null} if the field does not exist
     */
    public Object getTopLevelFieldValue(String fieldName) {
        FieldPath fieldPath = FieldPath.of(fieldName);
        Object root = fieldPath.initialContext(this);
        return getDirectChildFieldValue(root, fieldPath.getTrimmedPath());
    }

    /**
     * Sets the provided value to the top level field with the exact provided field name, whether it contains dots or not. If the field
     * does not exist, it will be created. If the field already exists, the value will be appended to its current value if the append
     * parameter is set to true, otherwise the existing value will be replaced.
     * @param fieldName the field name to set the value of
     * @param value the value to set
     * @param append whether to append the value to the existing value or replace it
     * @param allowDuplicates whether to allow duplicates in the field value
     */
    public void setTopLevelFieldValue(String fieldName, Object value, boolean append, boolean allowDuplicates) {
        FieldPath fieldPath = FieldPath.of(fieldName);
        Object root = fieldPath.initialContext(this);
        setDirectChildFieldValue(root, fieldPath.getTrimmedPath(), value, fieldPath.getTrimmedPath(), append, allowDuplicates);
    }

    /**
     * Appends the provided value to the top level field with the exact provided field name, whether it contains dots or not.
     * Any non-existing path element will be created.
     * If the path identifies a list, the value will be appended to the existing list.
     * If the path identifies a scalar, the scalar will be converted to a list and the provided value will be added to the newly created
     * list.
     * @param fieldName the field name to append the value to
     * @param value the value to append
     */
    public void appendTopLevelFieldValue(String fieldName, Object value) {
        FieldPath fieldPath = FieldPath.of(fieldName);
        Object root = fieldPath.initialContext(this);
        appendDirectChildFieldValue(root, fieldPath.getTrimmedPath(), value);
    }

    /**
     * Removes the top level field with the exact provided field name, whether it contains dots or not.
     * @param fieldName the field name to remove
     */
    public void removeTopLevelField(String fieldName) {
        FieldPath fieldPath = FieldPath.of(fieldName);
        Object root = fieldPath.initialContext(this);
        removeDirectChildField(root, fieldPath.getTrimmedPath(), fieldPath.getTrimmedPath());
    }

    /**
     * Collects all values from all document levels that match the provided path, assuming that any dot in the path can either represent
     * a separator between nested fields or be part of the field name itself.
     * Only values of the requested type are collected, otherwise they are ignored.
     * Numeric path elements are evaluated both as list indices and as field names when searching field values.
     * For example, given the following document:
     * <pre>
     *  {
     *      "foo": {
     *          "bar": {
     *              "baz": "value1",
     *              "qux": 1
     *          },
     *          "bar.baz": "value2",
     *          "qux": 2
     *      },
     *      "foo.bar": {
     *          "baz": 3,
     *          "qux": 3
     *      },
     *      "foo.bar.baz": "value4"
     *  }
     *  </pre>
     *  The expected returned list requested for path {@code foo.bar.baz} and type {@code String} would contain the following values:
     *  <ul>
     *      <li>value1</li>
     *      <li>value2</li>
     *      <li>value4</li>
     *  </ul>
     *  The value 3 is ignored because it is not of the requested type.
     * @param path the path to collect values by, where each dot can either represent a separator between nested fields or be part of the
     *             field name itself
     * @param clazz the type of the values to collect
     * @return a list of values that match the provided path and type
     * @param <T> the type of the values to collect
     */
    public <T> List<T> getAllFieldValues(String path, Class<T> clazz) {
        DottedFieldPaths fieldPaths = DottedFieldPaths.of(path);
        List<T> values = new ArrayList<>();
        collectAllFieldValues(fieldPaths.initialContext(this), fieldPaths.getPathSearchTree(), values, clazz, false);
        return values;
    }

    /**
     * Normalizes a field by collecting and removing all values from all document levels that match the provided path, assuming that any dot
     * in the path can either represent a separator between nested fields or be part of the field name itself. The collected values are
     * then set as a top level field with the normalized (possibly dotted) field name.
     * Only values of the requested type are collected, otherwise they are ignored.
     * Numeric path elements are evaluated both as list indices and as field names when collecting and removing field values.
     * For example, given the following document:
     * <pre>
     *  {
     *      "foo": {
     *          "bar": {
     *              "baz": "value1",
     *              "qux": 1
     *          },
     *          "bar.baz": "value2",
     *          "qux": 2
     *      },
     *      "foo.bar": {
     *          "baz": 3,
     *          "qux": 3
     *      },
     *      "foo.bar.baz": "value4"
     *  }
     *  </pre>
     *  The resulting document after normalizing by the path {@code foo.bar.baz} and type {@code String} would be:
     * <pre>
     *  {
     *      "foo": {
     *          "bar": {
     *              "qux": 1
     *          },
     *          "qux": 2
     *      },
     *      "foo.bar": {
     *          "baz": 3,
     *          "qux": 3
     *      },
     *      "foo.bar.baz": ["value1", "value2", "value4"]
     *  }
     *  </pre>
     *
     *  Since this API is intended for normalization processes, the search algorithm it relies on is designed to be efficient, with zero
     *  allocations in most cases, as it is expected to be called frequently and for general path searches, not necessarily paths that are
     *  expected to be present.
     * @param path the path to normalize by, where each dot can either represent a separator between nested fields or be part of field names
     * @param clazz the type of the values to normalize
     * @param <T> the type of the values to normalize
     */
    public <T> void normalizeField(String path, Class<T> clazz) {
        final DottedFieldPaths fieldPaths = DottedFieldPaths.of(path);
        Object root = fieldPaths.initialContext(this);

        // we use a thread local list to avoid creating a new list for each call, as in most cases the list will be empty
        List<?> genericValues = threadLocalValues.get();
        genericValues.clear();

        @SuppressWarnings("unchecked") // safe cast due to type erasure
        List<T> values = (List<T>) genericValues;
        collectAllFieldValues(root, fieldPaths.getPathSearchTree(), values, clazz, true);

        if (values.isEmpty() == false) {
            // Setting all values as a top level field with the normalized (possibly dotted) field name.
            // No need for a deep copy here, as we explicitly remove the found fields from the document during collection.
            // However, we must shallow copy the list as the value list is reused across multiple calls.
            Object finalValue = values.size() == 1 ? values.getFirst() : new ArrayList<>(values);
            try {
                // no need to append because the collect algorithm already collects and removes the value if exists in root level
                setDirectChildFieldValue(root, fieldPaths.getTrimmedPath(), finalValue, fieldPaths.getTrimmedPath(), false, true);
            } catch (Exception e) {
                // todo - shouldn't throw here
            }
        }
    }

    /**
     * NOTE: not using {@link #resolve(String, String, Object)} in order to avoid all allocations related to it. The difference between
     * the two approaches is that this API is designed to be used during general and frequent field searches and not for lookup of an
     * expected specific field. Therefore, we expect it to yield no results in the vast majority of cases.
     *
     * Returns the value of the direct child field if such with the exact full name exists. The difference from other APIs is that this
     * API does not resolve the full path, but only the direct child field, even if its name contains dots. In addition, it does not throw
     * an exception if the field is missing, but returns {@code null} instead.
     * @param parent the context object for this lookup
     * @param fieldName the name of the direct child field, may contain dots
     * @return the value of the direct child field if such with the exact full name exists or {@code null} if the field is missing
     */
    @Nullable
    private Object getDirectChildFieldValue(Object parent, String fieldName) {
        switch (parent) {
            case null -> throw new IllegalArgumentException(Errors.cannotResolve(fieldName, fieldName, null));
            case Map<?, ?> genericMap -> {
                @SuppressWarnings("unchecked")
                Map<String, Object> map = (Map<String, Object>) genericMap;
                Object value = map.getOrDefault(fieldName, NOT_FOUND); // getOrDefault is faster than containsKey + get
                if (value == NOT_FOUND) {
                    return null;
                } else {
                    return value;
                }
            }
            case List<?> list -> {
                int index;
                try {
                    index = Integer.parseInt(fieldName);
                } catch (NumberFormatException e) {
                    return null;
                }
                if (index < 0 || index >= list.size()) {
                    return null;
                }
                return list.get(index);
            }
            default -> {
                return null;
            }
        }
    }

    private <T> void collectAllFieldValues(Object context, PathNode pathNode, List<T> values, Class<T> clazz, boolean removeWhenFound) {
        ArrayList<PathNode> children = pathNode.getChildren();
        // noinspection ForLoopReplaceableByForEach - iterate without using an iterator to avoid allocations
        for (int i = 0; i < children.size(); i++) {
            PathNode child = children.get(i);
            Object fieldValue = null;
            try {
                fieldValue = getDirectChildFieldValue(context, child.getPathElement());
            } catch (Exception e) {
                // todo
            }
            if (fieldValue != null) {
                if (child.isLeaf()) {
                    T castValue;
                    try {
                        castValue = cast(child.getPathElement(), fieldValue, clazz);
                    } catch (Exception e) {
                        // if this field contains value that is not of the expected type, we ignore it
                        castValue = null;
                    }
                    if (castValue != null) {
                        values.add(castValue);
                    }
                    if (removeWhenFound) {
                        try {
                            removeDirectChildField(context, child.getPathElement(), child.getPathElement());
                        } catch (Exception e) {
                            // todo
                        }
                    }
                } else {
                    collectAllFieldValues(fieldValue, child, values, clazz, removeWhenFound);
                }
            }
        }
    }

    static final class PathNode {
        private final String pathElement;
        private final ArrayList<PathNode> children;

        PathNode(String pathElement) {
            this.pathElement = pathElement;
            this.children = new ArrayList<>();
        }

        public String getPathElement() {
            return pathElement;
        }

        public ArrayList<PathNode> getChildren() {
            return children;
        }

        public boolean isLeaf() {
            return children.isEmpty();
        }

        public void addChild(PathNode child) {
            children.add(child);
        }
    }

    static final class DottedFieldPaths extends FieldPath {

        private static final int MAX_SIZE = 512;
        private static final Map<String, DottedFieldPaths> CACHE = ConcurrentCollections.newConcurrentMapWithAggressiveConcurrency();

        // the search tree
        private final PathNode pathSearchTree;

        /**
         * Returns the search tree for the dotted field paths. Each node in the tree represents a path element, so that the paths
         * from the root to the leaf nodes represent all possible dotted field paths.
         * @return the search tree for the dotted field paths
         */
        public PathNode getPathSearchTree() {
            return pathSearchTree;
        }

        static DottedFieldPaths of(String path) {
            if (Strings.isEmpty(path)) {
                throw new IllegalArgumentException("path cannot be null nor empty");
            }
            DottedFieldPaths res = CACHE.get(path);
            if (res != null) {
                return res;
            }
            res = new DottedFieldPaths(path);
            if (CACHE.size() > MAX_SIZE) {
                CACHE.clear();
            }
            CACHE.put(path, res);
            return res;
        }

        // you shouldn't call this directly, use the DottedFieldPaths.of method above instead!
        private DottedFieldPaths(String path) {
            super(path);
            String trimmedPath = getTrimmedPath();
            // if the paths ends with a dot - remove it
            if (trimmedPath.endsWith(".")) {
                trimmedPath = trimmedPath.substring(0, trimmedPath.length() - 1);
            }
            this.pathSearchTree = new PathNode("");
            generatePathsRecursive(trimmedPath, 0, pathSearchTree);
        }

        /**
         * The recursive algorithm to generate the search tree for the dotted field paths - each dot is either considered as a separator or
         * as part of the element's name. This is done recursively, so that the eventual number of paths (leaf nodes) is equal to
         * 2^(number of dots in the path).
         *
         * @param path       the dotted field path
         * @param startIndex the start index from which to search the next dot
         * @param node       the current node in the search tree
         */
        private void generatePathsRecursive(String path, int startIndex, PathNode node) {
            int nextDot = path.indexOf('.', startIndex);
            if (nextDot > 0) {
                // Consider the next dot as a path separator and generate paths recursively for the rest of the path
                String element = path.substring(0, nextDot);
                PathNode child = new PathNode(element);
                node.addChild(child);
                generatePathsRecursive(path.substring(nextDot + 1), 0, child);

                // Consider the next dot as part of the path element's name
                generatePathsRecursive(path, nextDot + 1, node);
            } else {
                PathNode child = new PathNode(path);
                node.addChild(child);
            }
        }
    }
}
