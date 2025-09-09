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
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Base64;
import java.util.Collection;
import java.util.Collections;
import java.util.Date;
import java.util.Deque;
import java.util.HashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.function.BiConsumer;
import java.util.stream.Collectors;
import java.util.stream.Stream;

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
     * Maintains the stack of access patterns for each pipeline that this document is currently being processed by.
     * When a pipeline with one access pattern calls another pipeline with a different one, we must ensure the access patterns
     * are correctly restored when returning from a nested pipeline to an enclosing pipeline.
     */
    private final Deque<IngestPipelineFieldAccessPattern> accessPatternStack = new ArrayDeque<>();

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
         * The executedPipelines and accessPatternStack fields are clearly execution-centric rather than data centric.
         * Despite what the comment above says, we're copying it here anyway. THe reason is that this constructor is only called from
         * two non-test locations, and both of those involve the simulate pipeline logic. The simulate pipeline logic needs this
         * information. Rather than making the code more complicated, we're just copying them over here since it does no harm.
         */
        this.executedPipelines.addAll(other.executedPipelines);
        this.accessPatternStack.addAll(other.accessPatternStack);
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
        final FieldPath fieldPath = FieldPath.of(path, getCurrentAccessPatternSafe());
        Object context = fieldPath.initialContext(this);
        ResolveResult result = resolve(fieldPath.pathElements, fieldPath.pathElements.length, path, context, getCurrentAccessPatternSafe());
        if (result.wasSuccessful) {
            return cast(path, result.resolvedObject, clazz);
        } else if (ignoreMissing) {
            return null;
        } else {
            // Reconstruct the error message if the resolve result was incomplete
            throw new IllegalArgumentException(
                Objects.requireNonNullElseGet(result.errorMessage, () -> Errors.notPresent(path, result.missingFields))
            );
        }
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
        final FieldPath fieldPath = FieldPath.of(path, getCurrentAccessPatternSafe());
        Object context = fieldPath.initialContext(this);
        int leafKeyIndex = fieldPath.pathElements.length - 1;
        int lastContainerIndex = fieldPath.pathElements.length - 2;
        FieldPath.Element leafKey = fieldPath.pathElements[leafKeyIndex];
        for (int i = 0; i <= lastContainerIndex; i++) {
            FieldPath.Element pathElement = fieldPath.pathElements[i];
            if (context == null) {
                return false;
            } else if (context instanceof IngestCtxMap map) { // optimization: handle IngestCtxMap separately from Map
                switch (getCurrentAccessPatternSafe()) {
                    case CLASSIC -> context = map.get(pathElement.fieldName);
                    case FLEXIBLE -> {
                        if (pathElement.isArrayIndex()) {
                            return false;
                        }
                        Object object = map.getOrDefault(pathElement.fieldName, NOT_FOUND);
                        if (object != NOT_FOUND) {
                            context = object;
                        } else if (i == lastContainerIndex) {
                            // This is our last path element
                            if (leafKey.isArrayIndex()) {
                                // Our leaf element is an array index. which means that resolution of a list was required
                                return false;
                            }
                            // Update the leaf key to use this path element as a dotted prefix. Leave the context as it is.
                            leafKey = leafKey.prefix(pathElement.fieldName);
                        } else {
                            // Iterate through the remaining path elements, joining them with dots, until we get a hit
                            String combinedPath = pathElement.fieldName;
                            for (int j = i + 1; j <= lastContainerIndex; j++) {
                                var nextElement = fieldPath.pathElements[j];
                                if (nextElement.isArrayIndex()) {
                                    // We cannot chain array indices into field lookups.
                                    // If we have an array index here, we needed to find a list in the last check but didn't
                                    return false;
                                }
                                combinedPath = combinedPath + "." + nextElement.fieldName;
                                object = map.getOrDefault(combinedPath, NOT_FOUND); // getOrDefault is faster than containsKey + get
                                if (object != NOT_FOUND) {
                                    // Found one, update the outer loop index to skip past the elements we've used
                                    context = object;
                                    i = j;
                                    break;
                                }
                            }
                            if (object == NOT_FOUND) {
                                if (leafKey.isArrayIndex()) {
                                    // Our leaf element is an array index, which means that resolution of a list was required
                                    return false;
                                }
                                // Made it to the last path element without finding the field.
                                // Update the leaf key to use the visited combined path elements as a dotted prefix.
                                leafKey = leafKey.prefix(combinedPath);
                                // Update outer loop index to skip past the elements we've used
                                i = lastContainerIndex;
                            }
                        }
                    }
                }
            } else if (context instanceof Map<?, ?> map) {
                switch (getCurrentAccessPatternSafe()) {
                    case CLASSIC -> context = map.get(pathElement.fieldName);
                    case FLEXIBLE -> {
                        if (pathElement.isArrayIndex()) {
                            return false;
                        }
                        @SuppressWarnings("unchecked")
                        Map<String, Object> typedMap = (Map<String, Object>) context;
                        Object object = typedMap.getOrDefault(pathElement.fieldName, NOT_FOUND);
                        if (object != NOT_FOUND) {
                            context = object;
                        } else if (i == lastContainerIndex) {
                            // This is our last path element
                            if (leafKey.isArrayIndex()) {
                                // Our leaf element is an array index, which means that resolution of a list was required
                                return false;
                            }
                            // This is the last path element, update the leaf key to use this path element as a dotted prefix.
                            // Leave the context as it is.
                            leafKey = leafKey.prefix(pathElement.fieldName);
                        } else {
                            // Iterate through the remaining path elements, joining them with dots, until we get a hit
                            String combinedPath = pathElement.fieldName;
                            for (int j = i + 1; j <= lastContainerIndex; j++) {
                                var nextElement = fieldPath.pathElements[j];
                                if (nextElement.isArrayIndex()) {
                                    // We cannot chain array indices into field lookups.
                                    // If we have an array index here, we needed to find a list in the last check but didn't
                                    return false;
                                }
                                combinedPath = combinedPath + "." + nextElement;
                                object = typedMap.getOrDefault(combinedPath, NOT_FOUND); // getOrDefault is faster than containsKey + get
                                if (object != NOT_FOUND) {
                                    // Found one, update the outer loop index to skip past the elements we've used
                                    context = object;
                                    i = j;
                                    break;
                                }
                            }
                            if (object == NOT_FOUND) {
                                if (leafKey.isArrayIndex()) {
                                    // Our leaf element is an array index, which means that resolution of a list was required
                                    return false;
                                }
                                // Made it to the last path element without finding the field.
                                // Update the leaf key to use the visited combined path elements as a dotted prefix.
                                leafKey = leafKey.prefix(combinedPath);
                                // Update outer loop index to skip past the elements we've used.
                                i = lastContainerIndex;
                            }
                        }
                    }
                }
            } else if (context instanceof List<?> list) {
                int index;
                switch (getCurrentAccessPatternSafe()) {
                    case CLASSIC -> {
                        try {
                            index = Integer.parseInt(pathElement.fieldName);
                        } catch (NumberFormatException e) {
                            return false;
                        }
                    }
                    case FLEXIBLE -> {
                        // Flexible array access requires all array indices to be specified in square brackets.
                        if (pathElement.isFieldName()) {
                            return false;
                        }
                        index = pathElement.arrayIndex;
                    }
                    default -> {
                        // Sanity case
                        assert false : "Unsupported access pattern [" + getCurrentAccessPatternSafe() + "]";
                        return false;
                    }
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

        if (context == null) {
            return false;
        } else if (context instanceof IngestCtxMap map) { // optimization: handle IngestCtxMap separately from Map
            if (leafKey.isFieldName()) {
                return map.containsKey(leafKey.fieldName);
            } else {
                return false;
            }
        } else if (context instanceof Map<?, ?> map) {
            if (leafKey.isFieldName()) {
                return map.containsKey(leafKey.fieldName);
            } else {
                return false;
            }
        } else if (context instanceof List<?> list) {
            int index;
            switch (getCurrentAccessPatternSafe()) {
                case CLASSIC -> {
                    try {
                        index = Integer.parseInt(leafKey.fieldName);
                    } catch (NumberFormatException e) {
                        return false;
                    }
                }
                case FLEXIBLE -> {
                    // Flexible array access requires all array indices to be specified in square brackets.
                    if (leafKey.isFieldName()) {
                        return false;
                    }
                    index = leafKey.arrayIndex;
                }
                default -> {
                    // Sanity case
                    assert false : "Unsupported access pattern [" + getCurrentAccessPatternSafe() + "]";
                    return false;
                }
            }
            if (index >= 0 && index < list.size()) {
                return true;
            } else {
                if (failOutOfRange) {
                    throw new IllegalArgumentException(Errors.outOfBounds(path, index, list.size()));
                } else {
                    return false;
                }
            }
        } else {
            return false;
        }
    }

    /**
     * Removes the field identified by the provided path.
     *
     * @param path the path of the field to be removed
     * @throws IllegalArgumentException if the path is null, empty, invalid or if the field doesn't exist.
     */
    public void removeField(String path) {
        removeField(path, false);
    }

    /**
     * Removes the field identified by the provided path.
     *
     * @param path the path of the field to be removed
     * @param ignoreMissing The flag to determine whether to throw an exception when `path` is not found in the document.
     * @throws IllegalArgumentException if the path is null, empty, or invalid; or if the field doesn't exist (and ignoreMissing is false).
     */
    public void removeField(String path, boolean ignoreMissing) {
        final FieldPath fieldPath = FieldPath.of(path, getCurrentAccessPatternSafe());
        Object context = fieldPath.initialContext(this);
        FieldPath.Element leafKey = fieldPath.pathElements[fieldPath.pathElements.length - 1];
        ResolveResult result = resolve(
            fieldPath.pathElements,
            fieldPath.pathElements.length - 1,
            path,
            context,
            getCurrentAccessPatternSafe()
        );
        if (result.wasSuccessful) {
            context = result.resolvedObject;
        } else if (result.missingFields != null) {
            if (leafKey.isArrayIndex()) {
                if (ignoreMissing == false) {
                    // If we want to remove a leafKey that is an array index, we needed to find a list
                    throw new IllegalArgumentException(Errors.notPresent(path, result.missingFields));
                } else {
                    // couldn't find the array field, so there's nothing to remove :shrug:
                    return;
                }
            }
            // Incomplete result, update the leaf key and context to continue the operation
            leafKey = leafKey.prefix(result.missingFields);
            context = result.resolvedObject;
        } else if (ignoreMissing) {
            return; // nothing was found, so there's nothing to remove :shrug:
        } else {
            throw new IllegalArgumentException(result.errorMessage);
        }

        if (context == null && ignoreMissing == false) {
            throw new IllegalArgumentException(Errors.cannotRemove(path, leafKey.toString(), null));
        } else if (context instanceof IngestCtxMap map) { // optimization: handle IngestCtxMap separately from Map
            if (leafKey.isArrayIndex()) {
                if (ignoreMissing == false) {
                    throw new IllegalArgumentException(Errors.cannotResolve(path, leafKey.toString(), map));
                } else {
                    // ignoreMissing is true, so treat this as if we had just not found the field.
                    return;
                }
            }
            if (map.containsKey(leafKey.fieldName)) {
                map.remove(leafKey.fieldName);
            } else if (ignoreMissing == false) {
                throw new IllegalArgumentException(Errors.notPresent(path, leafKey.fieldName));
            }
        } else if (context instanceof Map<?, ?> map) {
            if (leafKey.isArrayIndex()) {
                if (ignoreMissing == false) {
                    throw new IllegalArgumentException(Errors.cannotResolve(path, leafKey.toString(), map));
                } else {
                    // ignoreMissing is true, so treat this as if we had just not found the field.
                    return;
                }
            }
            if (map.containsKey(leafKey.fieldName)) {
                map.remove(leafKey.fieldName);
            } else if (ignoreMissing == false) {
                throw new IllegalArgumentException(Errors.notPresent(path, leafKey.fieldName));
            }
        } else if (context instanceof List<?> list) {
            int index = -1;
            switch (getCurrentAccessPatternSafe()) {
                case CLASSIC -> {
                    try {
                        index = Integer.parseInt(leafKey.fieldName);
                    } catch (NumberFormatException e) {
                        if (ignoreMissing == false) {
                            throw new IllegalArgumentException(Errors.notInteger(path, leafKey.fieldName), e);
                        }
                    }
                }
                case FLEXIBLE -> {
                    if (leafKey.isFieldName()) {
                        if (ignoreMissing == false) {
                            throw new IllegalArgumentException(Errors.cannotResolve(path, leafKey.toString(), list));
                        } else {
                            // ignoreMissing is true, so treat this as if we had just not found the field.
                            return;
                        }
                    }
                    index = leafKey.arrayIndex;
                }
            }
            if (index < 0 || index >= list.size()) {
                if (ignoreMissing == false) {
                    throw new IllegalArgumentException(Errors.outOfBounds(path, index, list.size()));
                }
            } else {
                list.remove(index);
            }
        } else if (ignoreMissing == false) {
            throw new IllegalArgumentException(Errors.cannotRemove(path, leafKey.toString(), context));
        }
    }

    /**
     * Resolves the path elements (up to the limit) within the context. The result of such resolution can either be successful,
     * or can indicate a failure.
     */
    private static ResolveResult resolve(
        final FieldPath.Element[] pathElements,
        final int limit,
        final String fullPath,
        Object context,
        IngestPipelineFieldAccessPattern accessPattern
    ) {
        for (int i = 0; i < limit; i++) {
            FieldPath.Element pathElement = pathElements[i];
            if (context == null) {
                return ResolveResult.error(Errors.cannotResolve(fullPath, pathElement.toString(), null));
            } else if (context instanceof IngestCtxMap map) { // optimization: handle IngestCtxMap separately from Map
                switch (accessPattern) {
                    case CLASSIC -> {
                        Object object = map.getOrDefault(pathElement.fieldName, NOT_FOUND); // getOrDefault is faster than containsKey + get
                        if (object == NOT_FOUND) {
                            return ResolveResult.error(Errors.notPresent(fullPath, pathElement.toString()));
                        } else {
                            context = object;
                        }
                    }
                    case FLEXIBLE -> {
                        if (pathElement.isArrayIndex()) {
                            return ResolveResult.error(Errors.cannotResolveIndex(fullPath, pathElement.arrayIndex, context));
                        }
                        Object object = map.getOrDefault(pathElement.fieldName, NOT_FOUND); // getOrDefault is faster than containsKey + get
                        if (object != NOT_FOUND) {
                            context = object;
                        } else if (i == (limit - 1)) {
                            // This is our last path element, return incomplete
                            return ResolveResult.incomplete(context, pathElement.toString());
                        } else {
                            // Attempt a flexible lookup
                            // Iterate through the remaining elements until we get a hit
                            String combinedPath = pathElement.fieldName;
                            for (int j = i + 1; j < limit; j++) {
                                var nextElement = pathElements[j];
                                if (nextElement.isArrayIndex()) {
                                    // We cannot chain array indices into field lookups.
                                    // If we have an array index here, we needed to find a list in the last check but didn't
                                    return ResolveResult.error(Errors.notPresent(fullPath, combinedPath));
                                }
                                combinedPath = combinedPath + "." + nextElement.fieldName;
                                object = map.getOrDefault(combinedPath, NOT_FOUND); // getOrDefault is faster than containsKey + get
                                if (object != NOT_FOUND) {
                                    // Found one, update the outer loop index to skip past the elements we've used
                                    context = object;
                                    i = j;
                                    break;
                                }
                            }
                            if (object == NOT_FOUND) {
                                // Not found, and out of path elements, return an incomplete result
                                return ResolveResult.incomplete(context, combinedPath);
                            }
                        }
                    }
                }
            } else if (context instanceof Map<?, ?>) {
                switch (accessPattern) {
                    case CLASSIC -> {
                        @SuppressWarnings("unchecked")
                        Map<String, Object> map = (Map<String, Object>) context;
                        Object object = map.getOrDefault(pathElement.fieldName, NOT_FOUND); // getOrDefault is faster than containsKey + get
                        if (object == NOT_FOUND) {
                            return ResolveResult.error(Errors.notPresent(fullPath, pathElement.toString()));
                        } else {
                            context = object;
                        }
                    }
                    case FLEXIBLE -> {
                        if (pathElement.isArrayIndex()) {
                            return ResolveResult.error(Errors.cannotResolveIndex(fullPath, pathElement.arrayIndex, context));
                        }
                        @SuppressWarnings("unchecked")
                        Map<String, Object> map = (Map<String, Object>) context;
                        Object object = map.getOrDefault(pathElement.fieldName, NOT_FOUND); // getOrDefault is faster than containsKey + get
                        if (object != NOT_FOUND) {
                            context = object;
                        } else if (i == (limit - 1)) {
                            // This is our last path element, return incomplete
                            return ResolveResult.incomplete(context, pathElement.toString());
                        } else {
                            // Attempt a flexible lookup
                            // Iterate through the remaining elements until we get a hit
                            String combinedPath = pathElement.fieldName;
                            for (int j = i + 1; j < limit; j++) {
                                var nextElement = pathElements[j];
                                if (nextElement.isArrayIndex()) {
                                    // We cannot chain array indices into field lookups.
                                    // If we have an array index here, we needed to find a list in the last check but didn't
                                    return ResolveResult.error(Errors.notPresent(fullPath, combinedPath));
                                }
                                combinedPath = combinedPath + "." + nextElement.fieldName;
                                object = map.getOrDefault(combinedPath, NOT_FOUND); // getOrDefault is faster than containsKey + get
                                if (object != NOT_FOUND) {
                                    // Found one, update the outer loop index to skip past the elements we've used
                                    context = object;
                                    i = j;
                                    break;
                                }
                            }
                            if (object == NOT_FOUND) {
                                // Not found, and out of path elements, return an incomplete result
                                return ResolveResult.incomplete(context, combinedPath);
                            }
                        }
                    }
                }
            } else if (context instanceof List<?> list) {
                int index;
                switch (accessPattern) {
                    case CLASSIC -> {
                        // Classic array access uses integers after dot separators as an array accessor if context allows
                        try {
                            index = Integer.parseInt(pathElement.fieldName);
                        } catch (NumberFormatException e) {
                            return ResolveResult.error(Errors.notInteger(fullPath, pathElement.fieldName));
                        }
                    }
                    case FLEXIBLE -> {
                        // Flexible array access requires all array indices to be specified in square brackets.
                        if (pathElement.isFieldName()) {
                            // This is noted on the element object
                            return ResolveResult.error(Errors.cannotResolve(fullPath, pathElement.fieldName, list));
                        }
                        index = pathElement.arrayIndex;
                    }
                    default -> {
                        // Sanity case
                        assert false : "Unsupported access pattern [" + accessPattern + "]";
                        return ResolveResult.error(Errors.invalidPath(fullPath));
                    }
                }
                if (index < 0 || index >= list.size()) {
                    return ResolveResult.error(Errors.outOfBounds(fullPath, index, list.size()));
                } else {
                    context = list.get(index);
                }
            } else {
                return ResolveResult.error(Errors.cannotResolve(fullPath, pathElement.toString(), context));
            }
        }
        return ResolveResult.success(context);
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
        final FieldPath fieldPath = FieldPath.of(path, getCurrentAccessPatternSafe());
        Object context = fieldPath.initialContext(this);
        int leafKeyIndex = fieldPath.pathElements.length - 1;
        int lastContainerIndex = fieldPath.pathElements.length - 2;
        FieldPath.Element leafKey = fieldPath.pathElements[leafKeyIndex];
        for (int i = 0; i <= lastContainerIndex; i++) {
            FieldPath.Element pathElement = fieldPath.pathElements[i];
            if (context == null) {
                throw new IllegalArgumentException(Errors.cannotResolve(path, pathElement.toString(), null));
            } else if (context instanceof IngestCtxMap map) { // optimization: handle IngestCtxMap separately from Map
                switch (getCurrentAccessPatternSafe()) {
                    case CLASSIC -> {
                        Object object = map.getOrDefault(pathElement.fieldName, NOT_FOUND); // getOrDefault is faster than containsKey + get
                        if (object == NOT_FOUND) {
                            Map<Object, Object> newMap = new HashMap<>();
                            map.put(pathElement.fieldName, newMap);
                            context = newMap;
                        } else {
                            context = object;
                        }
                    }
                    case FLEXIBLE -> {
                        if (pathElement.isArrayIndex()) {
                            throw new IllegalArgumentException(Errors.cannotResolveIndex(path, pathElement.arrayIndex, context));
                        }
                        Object object = map.getOrDefault(pathElement.fieldName, NOT_FOUND); // getOrDefault is faster than containsKey + get
                        if (object != NOT_FOUND) {
                            context = object;
                        } else if (i == lastContainerIndex) {
                            // This is our last path element
                            if (leafKey.isArrayIndex()) {
                                // Our leaf element is an array index, which means that resolution of a list was required
                                throw new IllegalArgumentException(Errors.notPresent(path, pathElement.toString()));
                            }
                            // Update the leaf key to use this path element as a dotted prefix. Leave the context as it is.
                            leafKey = leafKey.prefix(pathElement.fieldName);
                        } else {
                            // Iterate through the remaining path elements, joining them with dots, until we get a hit
                            String combinedPath = pathElement.fieldName;
                            for (int j = i + 1; j <= lastContainerIndex; j++) {
                                var nextElement = fieldPath.pathElements[j];
                                if (nextElement.isArrayIndex()) {
                                    // We cannot chain array indices into field lookups.
                                    // If we have an array index here, we needed to find a list in the last check but didn't
                                    throw new IllegalArgumentException(Errors.notPresent(path, combinedPath));
                                }
                                combinedPath = combinedPath + "." + nextElement.fieldName;
                                object = map.getOrDefault(combinedPath, NOT_FOUND); // getOrDefault is faster than containsKey + get
                                if (object != NOT_FOUND) {
                                    // Found one, update the outer loop index to skip past the elements we've used
                                    context = object;
                                    i = j;
                                    break;
                                }
                            }
                            if (object == NOT_FOUND) {
                                if (leafKey.isArrayIndex()) {
                                    // Our leaf element is an array index, which means that resolution of a list was required
                                    throw new IllegalArgumentException(Errors.notPresent(path, combinedPath));
                                }
                                // Made it to the last path element without finding the field.
                                // Update the leaf key to use the visited combined path elements as a dotted prefix.
                                leafKey = leafKey.prefix(combinedPath);
                                // Update outer loop index to skip past the elements we've used
                                i = lastContainerIndex;
                            }
                        }
                    }
                }
            } else if (context instanceof Map<?, ?>) {
                switch (getCurrentAccessPatternSafe()) {
                    case CLASSIC -> {
                        @SuppressWarnings("unchecked")
                        Map<String, Object> map = (Map<String, Object>) context;
                        Object object = map.getOrDefault(pathElement.fieldName, NOT_FOUND); // getOrDefault is faster than containsKey + get
                        if (object == NOT_FOUND) {
                            Map<Object, Object> newMap = new HashMap<>();
                            map.put(pathElement.fieldName, newMap);
                            context = newMap;
                        } else {
                            context = object;
                        }
                    }
                    case FLEXIBLE -> {
                        if (pathElement.isArrayIndex()) {
                            throw new IllegalArgumentException(Errors.cannotResolveIndex(path, pathElement.arrayIndex, context));
                        }
                        @SuppressWarnings("unchecked")
                        Map<String, Object> map = (Map<String, Object>) context;
                        Object object = map.getOrDefault(pathElement.fieldName, NOT_FOUND); // getOrDefault is faster than containsKey + get
                        if (object != NOT_FOUND) {
                            context = object;
                        } else if (i == lastContainerIndex) {
                            // This is our last path element
                            if (leafKey.isArrayIndex()) {
                                // Our leaf element is an array index, which means that resolution of a list was required
                                throw new IllegalArgumentException(Errors.notPresent(path, pathElement.toString()));
                            }
                            // Update the leaf key to use this path element as a dotted prefix. Leave the context as it is.
                            leafKey = leafKey.prefix(pathElement.fieldName);
                        } else {
                            // Iterate through the remaining path elements, joining them with dots, until we get a hit
                            String combinedPath = pathElement.fieldName;
                            for (int j = i + 1; j <= lastContainerIndex; j++) {
                                var nextElement = fieldPath.pathElements[j];
                                if (nextElement.isArrayIndex()) {
                                    // We cannot chain array indices into field lookups.
                                    // If we have an array index here, we needed to find a list in the last check but didn't
                                    throw new IllegalArgumentException(Errors.notPresent(path, combinedPath));
                                }
                                combinedPath = combinedPath + "." + nextElement.fieldName;
                                object = map.getOrDefault(combinedPath, NOT_FOUND); // getOrDefault is faster than containsKey + get
                                if (object != NOT_FOUND) {
                                    // Found one, update the outer loop index to skip past the elements we've used
                                    context = object;
                                    i = j;
                                    break;
                                }
                            }
                            if (object == NOT_FOUND) {
                                if (leafKey.isArrayIndex()) {
                                    // Our leaf element is an array index, which means that resolution of a list was required
                                    throw new IllegalArgumentException(Errors.notPresent(path, combinedPath));
                                }
                                // Made it to the last path element without finding the field.
                                // Update the leaf key to use the visited combined path elements as a dotted prefix.
                                leafKey = leafKey.prefix(combinedPath);
                                // Update outer loop index to skip past the elements we've used
                                i = lastContainerIndex;
                            }
                        }
                    }
                }
            } else if (context instanceof List<?> list) {
                int index;
                switch (getCurrentAccessPatternSafe()) {
                    case CLASSIC -> {
                        try {
                            index = Integer.parseInt(pathElement.fieldName);
                        } catch (NumberFormatException e) {
                            throw new IllegalArgumentException(Errors.notInteger(path, pathElement.toString()), e);
                        }
                    }
                    case FLEXIBLE -> {
                        // Flexible array access requires all array indices to be specified in square brackets.
                        if (pathElement.isFieldName()) {
                            // This is noted on the element object
                            throw new IllegalArgumentException(Errors.cannotResolve(path, pathElement.fieldName, list));
                        }
                        index = pathElement.arrayIndex;
                    }
                    default -> {
                        // Sanity case
                        assert false : "Unsupported access pattern [" + getCurrentAccessPatternSafe() + "]";
                        throw new IllegalArgumentException(Errors.invalidPath(path));
                    }
                }
                if (index < 0 || index >= list.size()) {
                    throw new IllegalArgumentException(Errors.outOfBounds(path, index, list.size()));
                } else {
                    context = list.get(index);
                }
            } else {
                throw new IllegalArgumentException(Errors.cannotResolve(path, pathElement.toString(), context));
            }
        }

        if (context == null) {
            throw new IllegalArgumentException(Errors.cannotSet(path, leafKey.toString(), null));
        } else if (context instanceof IngestCtxMap map) { // optimization: handle IngestCtxMap separately from Map
            if (leafKey.isArrayIndex()) {
                throw new IllegalArgumentException(Errors.cannotResolveIndex(path, leafKey.arrayIndex, context));
            }
            if (append) {
                Object object = map.getOrDefault(leafKey.fieldName, NOT_FOUND); // getOrDefault is faster than containsKey + get
                if (object == NOT_FOUND) {
                    List<Object> list = new ArrayList<>();
                    appendValues(list, value);
                    map.put(leafKey.fieldName, list);
                } else {
                    Object list = appendValues(object, value, allowDuplicates);
                    if (list != object) {
                        map.put(leafKey.fieldName, list);
                    }
                }
                return;
            }
            map.put(leafKey.fieldName, value);
        } else if (context instanceof Map<?, ?>) {
            if (leafKey.isArrayIndex()) {
                throw new IllegalArgumentException(Errors.cannotResolveIndex(path, leafKey.arrayIndex, context));
            }
            @SuppressWarnings("unchecked")
            Map<String, Object> map = (Map<String, Object>) context;
            if (append) {
                Object object = map.getOrDefault(leafKey.fieldName, NOT_FOUND); // getOrDefault is faster than containsKey + get
                if (object == NOT_FOUND) {
                    List<Object> list = new ArrayList<>();
                    appendValues(list, value);
                    map.put(leafKey.fieldName, list);
                } else {
                    Object list = appendValues(object, value, allowDuplicates);
                    if (list != object) {
                        map.put(leafKey.fieldName, list);
                    }
                }
                return;
            }
            map.put(leafKey.fieldName, value);
        } else if (context instanceof List<?>) {
            @SuppressWarnings("unchecked")
            List<Object> list = (List<Object>) context;
            int index;
            switch (getCurrentAccessPatternSafe()) {
                case CLASSIC -> {
                    try {
                        index = Integer.parseInt(leafKey.fieldName);
                    } catch (NumberFormatException e) {
                        throw new IllegalArgumentException(Errors.notInteger(path, leafKey.fieldName), e);
                    }
                }
                case FLEXIBLE -> {
                    // Flexible array access requires all array indices to be specified in square brackets.
                    if (leafKey.isFieldName()) {
                        // This is noted on the element object
                        throw new IllegalArgumentException(Errors.cannotResolve(path, leafKey.toString(), list));
                    }
                    index = leafKey.arrayIndex;
                }
                default -> {
                    // Sanity case
                    assert false : "Unsupported access pattern [" + getCurrentAccessPatternSafe() + "]";
                    throw new IllegalArgumentException(Errors.invalidPath(path));
                }
            }
            if (index < 0 || index >= list.size()) {
                throw new IllegalArgumentException(Errors.outOfBounds(path, index, list.size()));
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
            throw new IllegalArgumentException(Errors.cannotSet(path, leafKey.toString(), context));
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
            IngestPipelineFieldAccessPattern previousAccessPattern = accessPatternStack.peek();
            accessPatternStack.push(pipeline.getFieldAccessPattern());
            pipeline.execute(this, (result, e) -> {
                executedPipelines.remove(pipeline.getId());
                accessPatternStack.poll();
                assert previousAccessPattern == accessPatternStack.peek()
                    : "Cleared access pattern from nested pipeline and found inconsistent stack state. Expected ["
                        + previousAccessPattern
                        + "] but found ["
                        + accessPatternStack.peek()
                        + "]";
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
     * @return The access pattern for any currently executing pipelines, or empty if no pipelines are in progress for this doc
     */
    public Optional<IngestPipelineFieldAccessPattern> getCurrentAccessPattern() {
        return Optional.ofNullable(accessPatternStack.peek());
    }

    /**
     * @return The access pattern for any currently executing pipelines, or {@link IngestPipelineFieldAccessPattern#CLASSIC} if no
     * pipelines are in progress for this doc for the sake of backwards compatibility
     */
    private IngestPipelineFieldAccessPattern getCurrentAccessPatternSafe() {
        return getCurrentAccessPattern().orElse(IngestPipelineFieldAccessPattern.CLASSIC);
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

    private static final class FieldPath {

        private record Element(String fieldName, Integer arrayIndex) {
            private static final String EMPTY_STRING = "";

            /**
             * Creates a new FieldPath Element which corresponds to a regular part of a field path.
             * @param fieldName name of the field to access, or a string to use for array indexing if running in CLASSIC access pattern.
             * @return A field name element
             */
            static Element field(String fieldName) {
                Objects.requireNonNull(fieldName, "fieldName cannot be null");
                if (fieldName.isEmpty()) {
                    throw new IllegalArgumentException("fieldName cannot be empty");
                }
                return new Element(fieldName, null);
            }

            /**
             * Creates a new FieldPath Element which corresponds to an array index specified by the square bracket syntax available
             * when using the {@link IngestPipelineFieldAccessPattern#FLEXIBLE} access pattern.
             * @param arrayIndex array index specified in square brackets
             * @return An array index element
             */
            static Element index(int arrayIndex) {
                if (arrayIndex < 0) {
                    throw new IndexOutOfBoundsException(arrayIndex);
                }
                return new Element(EMPTY_STRING, arrayIndex);
            }

            /**
             * @return true if this element is for accessing a regular field
             */
            boolean isFieldName() {
                return fieldName.isEmpty() == false && arrayIndex == null;
            }

            /**
             * @return true if this element is for an array index used for the FLEXIBLE access pattern
             */
            boolean isArrayIndex() {
                return isFieldName() == false;
            }

            /**
             * Creates a new Element combining the given prefix field with the current field name
             * @param prefixField field to use as a prefix to the current field
             * @return A new Element for a combined field name
             * @throws IllegalArgumentException if this Element is an array index
             */
            Element prefix(String prefixField) {
                if (isArrayIndex()) {
                    throw new IllegalArgumentException("Cannot prefix an array index element");
                }
                return field(prefixField + "." + fieldName);
            }

            @Override
            public String toString() {
                return isFieldName() ? fieldName : "[" + arrayIndex + "]";
            }
        }

        /**
         * A compound cache key for tracking previously parsed field paths
         * @param path The field path as given by the caller
         * @param accessPattern The access pattern used to parse the field path
         */
        private record CacheKey(String path, IngestPipelineFieldAccessPattern accessPattern) {}

        private static final int MAX_SIZE = 512;
        private static final Map<CacheKey, FieldPath> CACHE = ConcurrentCollections.newConcurrentMapWithAggressiveConcurrency();

        // constructing a new FieldPath requires that we parse a String (e.g. "foo.bar.baz") into an array
        // of path elements (e.g. ["foo", "bar", "baz"]). Calling String#split results in the allocation
        // of an ArrayList to hold the results, then a new String is created for each path element, and
        // then finally a String[] is allocated to hold the actual result -- in addition to all that, we
        // do some processing ourselves on the path and path elements to validate and prepare them.
        // the above CACHE and the below 'FieldPath.of' method allow us to almost always avoid this work.

        static FieldPath of(String path, IngestPipelineFieldAccessPattern accessPattern) {
            if (Strings.isEmpty(path)) {
                throw new IllegalArgumentException("path cannot be null nor empty");
            }
            CacheKey cacheKey = new CacheKey(path, accessPattern);
            FieldPath res = CACHE.get(cacheKey);
            if (res != null) {
                return res;
            }
            res = new FieldPath(path, accessPattern);
            if (CACHE.size() > MAX_SIZE) {
                CACHE.clear();
            }
            CACHE.put(cacheKey, res);
            return res;
        }

        private final Element[] pathElements;
        private final boolean useIngestContext;

        // you shouldn't call this directly, use the FieldPath.of method above instead!
        private FieldPath(String path, IngestPipelineFieldAccessPattern accessPattern) {
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
            String[] pathParts = newPath.split("\\.");
            this.pathElements = processPathParts(path, pathParts, accessPattern);
        }

        private static Element[] processPathParts(String fullPath, String[] pathParts, IngestPipelineFieldAccessPattern accessPattern) {
            if (pathParts.length == 1 && pathParts[0].isEmpty()) {
                throw new IllegalArgumentException("path [" + fullPath + "] is not valid");
            }
            return switch (accessPattern) {
                case CLASSIC -> Arrays.stream(pathParts).map(Element::field).toArray(Element[]::new);
                case FLEXIBLE -> parseFlexibleFields(fullPath, pathParts);
            };
        }

        /**
         * Parses path syntax that is specific to the {@link IngestPipelineFieldAccessPattern#FLEXIBLE} ingest doc access pattern. Supports
         * syntax like square bracket array access, which is the only way to index arrays in flexible mode.
         * @param fullPath The un-split path to use for error messages
         * @param pathParts The tokenized field path to parse
         * @return An array of Elements
         */
        private static Element[] parseFlexibleFields(String fullPath, String[] pathParts) {
            return Arrays.stream(pathParts).flatMap(pathPart -> {
                int openBracket = pathPart.indexOf('[');
                if (openBracket == -1) {
                    // Ensure there are no end bracket characters as they are also reserved
                    if (pathPart.indexOf(']') >= 0) {
                        throw new IllegalArgumentException("path [" + fullPath + "] is not valid");
                    }
                    return Stream.of(Element.field(pathPart));
                } else if (openBracket == 0) {
                    throw new IllegalArgumentException("path [" + fullPath + "] is not valid");
                } else {
                    List<Element> resultElements = new ArrayList<>();
                    String rootField = pathPart.substring(0, openBracket);
                    resultElements.add(Element.field(rootField));

                    boolean elementsRemain = true;
                    while (elementsRemain) {
                        int closeBracket = pathPart.indexOf(']', openBracket);
                        if (closeBracket <= openBracket) {
                            throw new IllegalArgumentException("path [" + fullPath + "] is not valid");
                        }

                        String rawIndex = pathPart.substring(openBracket + 1, closeBracket);
                        try {
                            resultElements.add(Element.index(Integer.parseInt(rawIndex)));
                        } catch (NumberFormatException numberFormatException) {
                            throw new IllegalArgumentException(Errors.notInteger(fullPath, rawIndex));
                        }

                        if (closeBracket == pathPart.length() - 1) {
                            elementsRemain = false;
                        } else {
                            if (pathPart.charAt(closeBracket + 1) != '[') {
                                throw new IllegalArgumentException("path [" + fullPath + "] is not valid");
                            }
                            openBracket = closeBracket + 1;
                        }
                    }
                    return resultElements.stream();
                }
            }).toArray(Element[]::new);
        }

        public Object initialContext(IngestDocument document) {
            return useIngestContext ? document.getIngestMetadata() : document.getCtxMap();
        }
    }

    private record ResolveResult(boolean wasSuccessful, Object resolvedObject, String errorMessage, String missingFields) {
        /**
         * The resolve operation ended with a successful result, locating the resolved object at the given path location
         * @param resolvedObject The resolved object
         * @return Successful result
         */
        static ResolveResult success(Object resolvedObject) {
            return new ResolveResult(true, resolvedObject, null, null);
        }

        /**
         * Due to the access pattern, the resolve operation was only partially completed. The last resolved context object is returned,
         * along with the fields that have been tried up until running into the field limit. The result's success flag is set to false,
         * but it contains additional information about further resolving the operation.
         * @param lastResolvedObject The last successfully resolved context object from the document
         * @param missingFields The fields from the given path that have not been located yet
         * @return Incomplete result
         */
        static ResolveResult incomplete(Object lastResolvedObject, String missingFields) {
            return new ResolveResult(false, lastResolvedObject, null, missingFields);
        }

        /**
         * The resolve operation ended with an error. The object at the given path location could not be resolved, either due to it
         * being missing, or the path being invalid.
         * @param errorMessage The error message to be returned.
         * @return Error result
         */
        static ResolveResult error(String errorMessage) {
            return new ResolveResult(false, null, errorMessage, null);
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

        private static String cannotResolveIndex(String path, int key, Object value) {
            if (value == null) {
                return "cannot resolve array index [" + key + "] from null as part of path [" + path + "]";
            } else {
                final String type = value.getClass().getName();
                return "cannot resolve array index [" + key + "] from object of type [" + type + "] as part of path [" + path + "]";
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

        private static String invalidPath(String fullPath) {
            return "path [" + fullPath + "] is not valid";
        }
    }
}
