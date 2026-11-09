/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.ingest;

import org.elasticsearch.common.util.CollectionUtils;
import org.elasticsearch.common.util.Maps;
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
import java.util.Collection;
import java.util.Collections;
import java.util.Deque;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.ListIterator;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.function.BiConsumer;
import java.util.stream.Collectors;

/**
 * Represents a single document being captured before indexing and holds the source and metadata (like id, type and index).
 * <p>
 * The methods that read or change a field (<code>getFieldValue</code>, <code>hasField</code>, <code>removeField</code>,
 * <code>appendFieldValue</code> and <code>setFieldValue</code>) come in two forms, one that takes the path as a {@link String} and one
 * that takes a {@link FieldPath}, and they behave the same. The {@link String} forms parse the path every time they are called (a cache
 * of recently parsed paths keeps that cheap), whereas a {@link FieldPath} is parsed once, when it is created. Prefer the
 * {@link FieldPath} forms wherever the path is known up front, as the fields that a processor is configured with are. The
 * {@link String} forms are not deprecated, and they are the natural choice when the path is only known at run time, for example when it
 * is taken from the contents of the document, or comes from a caller that has no use for a {@link FieldPath}.
 */
public final class IngestDocument {

    public static final String INGEST_KEY = "_ingest";
    public static final String SOURCE_KEY = SourceFieldMapper.NAME; // "_source"
    static final String INGEST_PREFIX = INGEST_KEY + ".";
    static final String SOURCE_PREFIX = SOURCE_KEY + ".";

    private static final String PIPELINE_CYCLE_ERROR_MESSAGE = "Cycle detected for pipeline: ";
    static final String TIMESTAMP = "timestamp";
    // This is the maximum number of nested pipelines that can be within a pipeline. If there are more, we bail out with an error
    public static final int MAX_PIPELINES = Integer.parseInt(System.getProperty("es.ingest.max_pipelines", "100"));

    // a 'not found' sentinel value for use in getOrDefault calls in order to avoid containsKey-and-then-get
    private static final Object NOT_FOUND = new Object();

    // what 'resolve' returns, rather than a ResolveResult, when the path could not be resolved and the caller does not care why. That
    // is the case for every caller that ignores missing fields, and not having to describe the failure (which would mean allocating a
    // ResolveResult, and building an error message that is then thrown away) makes that very common situation allocation free.
    private static final Object IGNORED_FAILURE = new Object();

    /**
     * What {@link #resolve} should do when a path can not be resolved.
     */
    private enum Failures {
        /** Describe the failure, with a {@link ResolveResult}, so that the caller can report it. */
        REPORT,
        /** Return {@link #IGNORED_FAILURE} for an error, but still describe an incomplete result, as the caller can make use of that. */
        IGNORE_ERRORS,
        /** Return {@link #IGNORED_FAILURE} for any failure, including an incomplete result, as the caller has no use for either. */
        IGNORE_ALL
    }

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
            new IngestCtxMap(
                CollectionUtils.deepCopy(ensureNoSelfReferences(other.ctxMap.getSource(), "source document")),
                other.ctxMap.getMetadata().clone()
            ),
            CollectionUtils.deepCopy(ensureNoSelfReferences(other.ingestMetadata, "ingest metadata"))
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
    private static Map<String, Object> ensureNoSelfReferences(Map<String, Object> map, String hint) {
        CollectionUtils.ensureNoSelfReferences(map, hint);
        return map;
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
        return getFieldValue(FieldPath.parse(path), clazz, ignoreMissing);
    }

    /**
     * Same as {@link #getFieldValue(String, Class)} but takes a path that has already been parsed, see {@link FieldPath}.
     */
    public <T> T getFieldValue(FieldPath fieldPath, Class<T> clazz) {
        return getFieldValue(fieldPath, clazz, false);
    }

    /**
     * Same as {@link #getFieldValue(String, Class, boolean)} but takes a path that has already been parsed, see {@link FieldPath}.
     */
    public <T> T getFieldValue(FieldPath fieldPath, Class<T> clazz, boolean ignoreMissing) {
        final FieldPath parsedPath = fieldPath.resolve(this);
        final String path = parsedPath.path;
        final IngestPipelineFieldAccessPattern accessPattern = getAccessPattern();
        parsedPath.validate(accessPattern);
        Object context = parsedPath.initialContext(this);
        Object result = resolve(
            parsedPath.pathElements,
            parsedPath.pathElements.length,
            path,
            context,
            accessPattern,
            ignoreMissing ? Failures.IGNORE_ALL : Failures.REPORT
        );
        if (result == IGNORED_FAILURE) {
            return null;
        } else if (result instanceof ResolveResult failure) {
            assert ignoreMissing == false : "resolve would have ignored this failure";
            // Reconstruct the error message if the resolve result was incomplete
            throw new IllegalArgumentException(
                Objects.requireNonNullElseGet(failure.errorMessage, () -> Errors.notPresent(path, failure.missingFields))
            );
        } else {
            return cast(path, result, clazz);
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
        return hasField(FieldPath.parse(path), failOutOfRange);
    }

    /**
     * Same as {@link #hasField(String)} but takes a path that has already been parsed, see {@link FieldPath}.
     */
    public boolean hasField(FieldPath fieldPath) {
        return hasField(fieldPath, false);
    }

    /**
     * Same as {@link #hasField(String, boolean)} but takes a path that has already been parsed, see {@link FieldPath}.
     */
    public boolean hasField(FieldPath fieldPath, boolean failOutOfRange) {
        final FieldPath parsedPath = fieldPath.resolve(this);
        final String path = parsedPath.path;
        final IngestPipelineFieldAccessPattern accessPattern = getAccessPattern();
        parsedPath.validate(accessPattern);
        Object context = parsedPath.initialContext(this);
        int leafKeyIndex = parsedPath.pathElements.length - 1;
        int lastContainerIndex = parsedPath.pathElements.length - 2;
        String leafKey = parsedPath.pathElements[leafKeyIndex];
        for (int i = 0; i <= lastContainerIndex; i++) {
            String pathElement = parsedPath.pathElements[i];
            if (context == null) {
                return false;
            } else if (context instanceof IngestCtxMap map) { // optimization: handle IngestCtxMap separately from Map
                switch (accessPattern) {
                    case CLASSIC -> context = map.get(pathElement);
                    case FLEXIBLE -> {
                        Object object = map.getOrDefault(pathElement, NOT_FOUND);
                        if (object != NOT_FOUND) {
                            context = object;
                        } else if (i == lastContainerIndex) {
                            // This is the last path element, update the leaf key to use this path element as a dotted prefix.
                            // Leave the context as it is.
                            leafKey = pathElement + "." + leafKey;
                        } else {
                            // Iterate through the remaining path elements, joining them with dots, until we get a hit
                            String combinedPath = pathElement;
                            for (int j = i + 1; j <= lastContainerIndex; j++) {
                                combinedPath = combinedPath + "." + parsedPath.pathElements[j];
                                object = map.getOrDefault(combinedPath, NOT_FOUND); // getOrDefault is faster than containsKey + get
                                if (object != NOT_FOUND) {
                                    // Found one, update the outer loop index to skip past the elements we've used
                                    context = object;
                                    i = j;
                                    break;
                                }
                            }
                            if (object == NOT_FOUND) {
                                // Made it to the last path element without finding the field.
                                // Update the leaf key to use the visited combined path elements as a dotted prefix.
                                leafKey = combinedPath + "." + leafKey;
                                // Update outer loop index to skip past the elements we've used
                                i = lastContainerIndex;
                            }
                        }
                    }
                }
            } else if (context instanceof Map<?, ?> map) {
                switch (accessPattern) {
                    case CLASSIC -> context = map.get(pathElement);
                    case FLEXIBLE -> {
                        @SuppressWarnings("unchecked")
                        Map<String, Object> typedMap = (Map<String, Object>) context;
                        Object object = typedMap.getOrDefault(pathElement, NOT_FOUND);
                        if (object != NOT_FOUND) {
                            context = object;
                        } else if (i == lastContainerIndex) {
                            // This is the last path element, update the leaf key to use this path element as a dotted prefix.
                            // Leave the context as it is.
                            leafKey = pathElement + "." + leafKey;
                        } else {
                            // Iterate through the remaining path elements, joining them with dots, until we get a hit
                            String combinedPath = pathElement;
                            for (int j = i + 1; j <= lastContainerIndex; j++) {
                                combinedPath = combinedPath + "." + parsedPath.pathElements[j];
                                object = typedMap.getOrDefault(combinedPath, NOT_FOUND); // getOrDefault is faster than containsKey + get
                                if (object != NOT_FOUND) {
                                    // Found one, update the outer loop index to skip past the elements we've used
                                    context = object;
                                    i = j;
                                    break;
                                }
                            }
                            if (object == NOT_FOUND) {
                                // Made it to the last path element without finding the field.
                                // Update the leaf key to use the visited combined path elements as a dotted prefix.
                                leafKey = combinedPath + "." + leafKey;
                                // Update outer loop index to skip past the elements we've used.
                                i = lastContainerIndex;
                            }
                        }
                    }
                }
            } else if (context instanceof List<?> list) {
                if (accessPattern == IngestPipelineFieldAccessPattern.FLEXIBLE) {
                    // Flexible access pattern cannot yet access array values, new syntax must be added.
                    // Handle this as if the path element was not parsable as an integer in the classic mode
                    return false;
                }
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

        if (context == null) {
            return false;
        } else if (context instanceof IngestCtxMap map) { // optimization: handle IngestCtxMap separately from Map
            return map.containsKey(leafKey);
        } else if (context instanceof Map<?, ?> map) {
            return map.containsKey(leafKey);
        } else if (context instanceof List<?> list) {
            if (accessPattern == IngestPipelineFieldAccessPattern.FLEXIBLE) {
                // Flexible access pattern cannot yet access array values, new syntax must be added.
                // Handle this as if the path element was not parsable as an integer in the classic mode
                return false;
            }
            try {
                int index = Integer.parseInt(leafKey);
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
        removeField(FieldPath.parse(path), ignoreMissing);
    }

    /**
     * Same as {@link #removeField(String)} but takes a path that has already been parsed, see {@link FieldPath}.
     */
    public void removeField(FieldPath fieldPath) {
        removeField(fieldPath, false);
    }

    /**
     * Same as {@link #removeField(String, boolean)} but takes a path that has already been parsed, see {@link FieldPath}.
     */
    public void removeField(FieldPath fieldPath, boolean ignoreMissing) {
        final FieldPath parsedPath = fieldPath.resolve(this);
        final String path = parsedPath.path;
        final IngestPipelineFieldAccessPattern accessPattern = getAccessPattern();
        parsedPath.validate(accessPattern);
        Object context = parsedPath.initialContext(this);
        String leafKey = parsedPath.pathElements[parsedPath.pathElements.length - 1];
        Object result = resolve(
            parsedPath.pathElements,
            parsedPath.pathElements.length - 1,
            path,
            context,
            accessPattern,
            ignoreMissing ? Failures.IGNORE_ERRORS : Failures.REPORT
        );
        if (result == IGNORED_FAILURE) {
            return; // nothing was found, so there's nothing to remove :shrug:
        } else if (result instanceof ResolveResult failure) {
            if (failure.missingFields != null) {
                // Incomplete result, update the leaf key and context to continue the operation
                leafKey = failure.missingFields + "." + leafKey;
                context = failure.resolvedObject;
            } else {
                assert ignoreMissing == false : "resolve would have ignored this error";
                throw new IllegalArgumentException(failure.errorMessage);
            }
        } else {
            context = result;
        }

        if (context == null && ignoreMissing == false) {
            throw new IllegalArgumentException(Errors.cannotRemove(path, leafKey, null));
        } else if (context instanceof IngestCtxMap map) { // optimization: handle IngestCtxMap separately from Map
            if (map.containsKey(leafKey)) {
                map.remove(leafKey);
            } else if (ignoreMissing == false) {
                throw new IllegalArgumentException(Errors.notPresent(path, leafKey));
            }
        } else if (context instanceof Map<?, ?> map) {
            if (map.containsKey(leafKey)) {
                map.remove(leafKey);
            } else if (ignoreMissing == false) {
                throw new IllegalArgumentException(Errors.notPresent(path, leafKey));
            }
        } else if (context instanceof List<?> list) {
            if (accessPattern == IngestPipelineFieldAccessPattern.FLEXIBLE) {
                // Flexible access pattern cannot yet access array values, new syntax must be added.
                if (ignoreMissing == false) {
                    throw new IllegalArgumentException("path [" + path + "] is not valid");
                } else {
                    // ignoreMissing is true, so treat this as if we had just not found the field.
                    return;
                }
            }
            int index = -1;
            try {
                index = Integer.parseInt(leafKey);
            } catch (NumberFormatException e) {
                if (ignoreMissing == false) {
                    throw new IllegalArgumentException(Errors.notInteger(path, leafKey), e);
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
            throw new IllegalArgumentException(Errors.cannotRemove(path, leafKey, context));
        }
    }

    /**
     * Resolves the path elements (up to the limit) within the context. The result is one of:
     * <ul>
     * <li>the object that was found (which can be null), if the resolution was successful. This is not wrapped in anything, so that the
     * (by far) most common outcome does not allocate.</li>
     * <li>{@link #IGNORED_FAILURE}, if the resolution failed and <code>failures</code> says that the caller does not care why. There is
     * no need to describe such a failure, which would allocate. An incomplete result (see {@link ResolveResult#incomplete}) is only
     * ignored if <code>failures</code> is {@link Failures#IGNORE_ALL}, as some callers that ignore errors can still use it.</li>
     * <li>a {@link ResolveResult} describing the failure, otherwise.</li>
     * </ul>
     */
    private static Object resolve(
        final String[] pathElements,
        final int limit,
        final String fullPath,
        Object context,
        IngestPipelineFieldAccessPattern accessPattern,
        Failures failures
    ) {
        for (int i = 0; i < limit; i++) {
            String pathElement = pathElements[i];
            if (context == null) {
                return failures != Failures.REPORT
                    ? IGNORED_FAILURE
                    : ResolveResult.error(Errors.cannotResolve(fullPath, pathElement, null));
            } else if (context instanceof IngestCtxMap map) { // optimization: handle IngestCtxMap separately from Map
                switch (accessPattern) {
                    case CLASSIC -> {
                        Object object = map.getOrDefault(pathElement, NOT_FOUND); // getOrDefault is faster than containsKey + get
                        if (object == NOT_FOUND) {
                            return failures != Failures.REPORT
                                ? IGNORED_FAILURE
                                : ResolveResult.error(Errors.notPresent(fullPath, pathElement));
                        } else {
                            context = object;
                        }
                    }
                    case FLEXIBLE -> {
                        Object object = map.getOrDefault(pathElement, NOT_FOUND); // getOrDefault is faster than containsKey + get
                        if (object != NOT_FOUND) {
                            context = object;
                        } else if (i == (limit - 1)) {
                            // This is our last path element, return incomplete
                            return failures == Failures.IGNORE_ALL ? IGNORED_FAILURE : ResolveResult.incomplete(context, pathElement);
                        } else {
                            // Attempt a flexible lookup
                            // Iterate through the remaining elements until we get a hit
                            String combinedPath = pathElement;
                            for (int j = i + 1; j < limit; j++) {
                                combinedPath = combinedPath + "." + pathElements[j];
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
                                return failures == Failures.IGNORE_ALL ? IGNORED_FAILURE : ResolveResult.incomplete(context, combinedPath);
                            }
                        }
                    }
                }
            } else if (context instanceof Map<?, ?>) {
                switch (accessPattern) {
                    case CLASSIC -> {
                        @SuppressWarnings("unchecked")
                        Map<String, Object> map = (Map<String, Object>) context;
                        Object object = map.getOrDefault(pathElement, NOT_FOUND); // getOrDefault is faster than containsKey + get
                        if (object == NOT_FOUND) {
                            return failures != Failures.REPORT
                                ? IGNORED_FAILURE
                                : ResolveResult.error(Errors.notPresent(fullPath, pathElement));
                        } else {
                            context = object;
                        }
                    }
                    case FLEXIBLE -> {
                        @SuppressWarnings("unchecked")
                        Map<String, Object> map = (Map<String, Object>) context;
                        Object object = map.getOrDefault(pathElement, NOT_FOUND); // getOrDefault is faster than containsKey + get
                        if (object != NOT_FOUND) {
                            context = object;
                        } else if (i == (limit - 1)) {
                            // This is our last path element, return incomplete
                            return failures == Failures.IGNORE_ALL ? IGNORED_FAILURE : ResolveResult.incomplete(context, pathElement);
                        } else {
                            // Attempt a flexible lookup
                            // Iterate through the remaining elements until we get a hit
                            String combinedPath = pathElement;
                            for (int j = i + 1; j < limit; j++) {
                                combinedPath = combinedPath + "." + pathElements[j];
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
                                return failures == Failures.IGNORE_ALL ? IGNORED_FAILURE : ResolveResult.incomplete(context, combinedPath);
                            }
                        }
                    }
                }
            } else if (context instanceof List<?> list) {
                if (accessPattern == IngestPipelineFieldAccessPattern.FLEXIBLE) {
                    // Flexible access pattern cannot yet access array values, new syntax must be added.
                    return failures != Failures.REPORT ? IGNORED_FAILURE : ResolveResult.error(Errors.invalidPath(fullPath));
                }
                int index;
                try {
                    index = Integer.parseInt(pathElement);
                } catch (NumberFormatException e) {
                    return failures != Failures.REPORT ? IGNORED_FAILURE : ResolveResult.error(Errors.notInteger(fullPath, pathElement));
                }
                if (index < 0 || index >= list.size()) {
                    return failures != Failures.REPORT
                        ? IGNORED_FAILURE
                        : ResolveResult.error(Errors.outOfBounds(fullPath, index, list.size()));
                } else {
                    context = list.get(index);
                }
            } else {
                return failures != Failures.REPORT
                    ? IGNORED_FAILURE
                    : ResolveResult.error(Errors.cannotResolve(fullPath, pathElement, context));
            }
        }
        return context;
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
        setFieldValue(FieldPath.parse(path), value, true, allowDuplicates, false);
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
     * @param ignoreEmptyValues When true, values that resolve to empty strings will not be added
     * @throws IllegalArgumentException if the path is null, empty or invalid.
     */
    public void appendFieldValue(String path, Object value, boolean allowDuplicates, boolean ignoreEmptyValues) {
        setFieldValue(FieldPath.parse(path), value, true, allowDuplicates, ignoreEmptyValues);
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
     * @param ignoreEmptyValues When true, values that resolve to empty strings will not be added
     * @throws IllegalArgumentException if the path is null, empty or invalid.
     */
    public void appendFieldValue(String path, ValueSource valueSource, boolean allowDuplicates, boolean ignoreEmptyValues) {
        appendFieldValue(path, valueSource.copyAndResolve(templateModel), allowDuplicates, ignoreEmptyValues);
    }

    /**
     * Same as {@link #appendFieldValue(String, Object)} but takes a path that has already been parsed, see {@link FieldPath}.
     */
    public void appendFieldValue(FieldPath fieldPath, Object value) {
        appendFieldValue(fieldPath, value, true);
    }

    /**
     * Same as {@link #appendFieldValue(String, Object, boolean)} but takes a path that has already been parsed, see {@link FieldPath}.
     */
    public void appendFieldValue(FieldPath fieldPath, Object value, boolean allowDuplicates) {
        setFieldValue(fieldPath, value, true, allowDuplicates, false);
    }

    /**
     * Same as {@link #appendFieldValue(String, Object, boolean, boolean)} but takes a path that has already been parsed, see
     * {@link FieldPath}.
     */
    public void appendFieldValue(FieldPath fieldPath, Object value, boolean allowDuplicates, boolean ignoreEmptyValues) {
        setFieldValue(fieldPath, value, true, allowDuplicates, ignoreEmptyValues);
    }

    /**
     * Same as {@link #appendFieldValue(String, ValueSource, boolean, boolean)} but takes a path that has already been parsed, see
     * {@link FieldPath}.
     */
    public void appendFieldValue(FieldPath fieldPath, ValueSource valueSource, boolean allowDuplicates, boolean ignoreEmptyValues) {
        appendFieldValue(fieldPath, valueSource.copyAndResolve(templateModel), allowDuplicates, ignoreEmptyValues);
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
        setFieldValue(FieldPath.parse(path), value, false, false, false);
    }

    /**
     * Same as {@link #setFieldValue(String, Object)} but takes a path that has already been parsed, see {@link FieldPath}.
     */
    public void setFieldValue(FieldPath fieldPath, Object value) {
        setFieldValue(fieldPath, value, false, false, false);
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
     * Same as {@link #setFieldValue(String, ValueSource)} but takes a path that has already been parsed, see {@link FieldPath}.
     */
    public void setFieldValue(FieldPath fieldPath, ValueSource valueSource) {
        setFieldValue(fieldPath, valueSource.copyAndResolve(templateModel));
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
        if (valueSource instanceof ValueSource.TemplatedValue) {
            if (ignoreEmptyValue == false || valueNotEmpty(value)) {
                setFieldValue(path, value);
            }
        } else {
            // it may seem a little surprising to not bother checking ignoreEmptyValue value here.
            // but this corresponds to the case of, e.g., a set processor with a literal value.
            // so if you have `"value": ""` and `"ignore_empty_value": true` right next to each other
            // in your processor definition, then, well, that's on you for being a bit silly. ;)
            setFieldValue(path, value);
        }
    }

    /**
     * Same as {@link #setFieldValue(String, ValueSource, boolean)} but takes a path that has already been parsed, see {@link FieldPath}.
     * <p>
     * As with that method, nothing is done if <code>ignoreEmptyValue</code> is set and the value that is produced is empty. That includes
     * resolving the path, so a path that is malformed is not reported in that case.
     */
    public void setFieldValue(FieldPath fieldPath, ValueSource valueSource, boolean ignoreEmptyValue) {
        Object value = valueSource.copyAndResolve(templateModel);
        if (valueSource instanceof ValueSource.TemplatedValue) {
            if (ignoreEmptyValue == false || valueNotEmpty(value)) {
                setFieldValue(fieldPath, value);
            }
        } else {
            // see the comment in the String path version of this method
            setFieldValue(fieldPath, value);
        }
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
        if (ignoreEmptyValue == false || valueNotEmpty(value)) {
            setFieldValue(path, value);
        }
    }

    /**
     * Same as {@link #setFieldValue(String, Object, boolean)} but takes a path that has already been parsed, see {@link FieldPath}.
     * <p>
     * As with that method, nothing is done if <code>ignoreEmptyValue</code> is set and the value is empty. That includes resolving the
     * path, so a path that is malformed is not reported in that case.
     */
    public void setFieldValue(FieldPath fieldPath, Object value, boolean ignoreEmptyValue) {
        if (ignoreEmptyValue == false || valueNotEmpty(value)) {
            setFieldValue(fieldPath, value);
        }
    }

    private void setFieldValue(FieldPath fieldPath, Object value, boolean append, boolean allowDuplicates, boolean ignoreEmptyValues) {
        assert append || (allowDuplicates == false && ignoreEmptyValues == false)
            : "allowDuplicates and ignoreEmptyValues only apply if append is true";
        final FieldPath parsedPath = fieldPath.resolve(this);
        final String path = parsedPath.path;
        final IngestPipelineFieldAccessPattern accessPattern = getAccessPattern();
        parsedPath.validate(accessPattern);
        Object context = parsedPath.initialContext(this);
        int leafKeyIndex = parsedPath.pathElements.length - 1;
        int lastContainerIndex = parsedPath.pathElements.length - 2;
        String leafKey = parsedPath.pathElements[leafKeyIndex];
        for (int i = 0; i <= lastContainerIndex; i++) {
            String pathElement = parsedPath.pathElements[i];
            if (context == null) {
                throw new IllegalArgumentException(Errors.cannotResolve(path, pathElement, null));
            } else if (context instanceof IngestCtxMap map) { // optimization: handle IngestCtxMap separately from Map
                switch (accessPattern) {
                    case CLASSIC -> {
                        Object object = map.getOrDefault(pathElement, NOT_FOUND); // getOrDefault is faster than containsKey + get
                        if (object == NOT_FOUND) {
                            Map<Object, Object> newMap = new HashMap<>();
                            map.put(pathElement, newMap);
                            context = newMap;
                        } else {
                            context = object;
                        }
                    }
                    case FLEXIBLE -> {
                        Object object = map.getOrDefault(pathElement, NOT_FOUND); // getOrDefault is faster than containsKey + get
                        if (object != NOT_FOUND) {
                            context = object;
                        } else if (i == lastContainerIndex) {
                            // This is our last path element, update the leaf key to use this path element as a dotted prefix.
                            // Leave the context as it is.
                            leafKey = pathElement + "." + leafKey;
                        } else {
                            // Iterate through the remaining path elements, joining them with dots, until we get a hit
                            String combinedPath = pathElement;
                            for (int j = i + 1; j <= lastContainerIndex; j++) {
                                combinedPath = combinedPath + "." + parsedPath.pathElements[j];
                                object = map.getOrDefault(combinedPath, NOT_FOUND); // getOrDefault is faster than containsKey + get
                                if (object != NOT_FOUND) {
                                    // Found one, update the outer loop index to skip past the elements we've used
                                    context = object;
                                    i = j;
                                    break;
                                }
                            }
                            if (object == NOT_FOUND) {
                                // Made it to the last path element without finding the field.
                                // Update the leaf key to use the visited combined path elements as a dotted prefix.
                                leafKey = combinedPath + "." + leafKey;
                                // Update outer loop index to skip past the elements we've used
                                i = lastContainerIndex;
                            }
                        }
                    }
                }
            } else if (context instanceof Map<?, ?>) {
                switch (accessPattern) {
                    case CLASSIC -> {
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
                    }
                    case FLEXIBLE -> {
                        @SuppressWarnings("unchecked")
                        Map<String, Object> map = (Map<String, Object>) context;
                        Object object = map.getOrDefault(pathElement, NOT_FOUND); // getOrDefault is faster than containsKey + get
                        if (object != NOT_FOUND) {
                            context = object;
                        } else if (i == lastContainerIndex) {
                            // This is our last path element, update the leaf key to use this path element as a dotted prefix.
                            // Leave the context as it is.
                            leafKey = pathElement + "." + leafKey;
                        } else {
                            // Iterate through the remaining path elements, joining them with dots, until we get a hit
                            String combinedPath = pathElement;
                            for (int j = i + 1; j <= lastContainerIndex; j++) {
                                combinedPath = combinedPath + "." + parsedPath.pathElements[j];
                                object = map.getOrDefault(combinedPath, NOT_FOUND); // getOrDefault is faster than containsKey + get
                                if (object != NOT_FOUND) {
                                    // Found one, update the outer loop index to skip past the elements we've used
                                    context = object;
                                    i = j;
                                    break;
                                }
                            }
                            if (object == NOT_FOUND) {
                                // Made it to the last path element without finding the field.
                                // Update the leaf key to use the visited combined path elements as a dotted prefix.
                                leafKey = combinedPath + "." + leafKey;
                                // Update outer loop index to skip past the elements we've used
                                i = lastContainerIndex;
                            }
                        }
                    }
                }
            } else if (context instanceof List<?> list) {
                if (accessPattern == IngestPipelineFieldAccessPattern.FLEXIBLE) {
                    // Flexible access pattern cannot yet access array values, new syntax must be added.
                    throw new IllegalArgumentException("path [" + path + "] is not valid");
                }
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

        if (context == null) {
            throw new IllegalArgumentException(Errors.cannotSet(path, leafKey, null));
        } else if (context instanceof IngestCtxMap map) { // optimization: handle IngestCtxMap separately from Map
            if (append) {
                Object object = map.getOrDefault(leafKey, NOT_FOUND); // getOrDefault is faster than containsKey + get
                if (object == NOT_FOUND) {
                    List<Object> list = new ArrayList<>();
                    appendValues(list, value, allowDuplicates, ignoreEmptyValues);
                    if (list.isEmpty() == false) {
                        map.put(leafKey, list);
                    }
                } else {
                    Object list = appendValues(object, value, allowDuplicates, ignoreEmptyValues);
                    if (list != object) {
                        map.put(leafKey, list);
                    }
                }
                return;
            }
            map.put(leafKey, value);
        } else if (context instanceof Map<?, ?>) {
            @SuppressWarnings("unchecked")
            Map<String, Object> map = (Map<String, Object>) context;
            if (append) {
                Object object = map.getOrDefault(leafKey, NOT_FOUND); // getOrDefault is faster than containsKey + get
                if (object == NOT_FOUND) {
                    List<Object> list = new ArrayList<>();
                    appendValues(list, value, allowDuplicates, ignoreEmptyValues);
                    if (list.isEmpty() == false) {
                        map.put(leafKey, list);
                    }
                } else {
                    Object list = appendValues(object, value, allowDuplicates, ignoreEmptyValues);
                    if (list != object) {
                        map.put(leafKey, list);
                    }
                }
                return;
            }
            map.put(leafKey, value);
        } else if (context instanceof List<?>) {
            if (accessPattern == IngestPipelineFieldAccessPattern.FLEXIBLE) {
                // Flexible access pattern cannot yet access array values, new syntax must be added.
                throw new IllegalArgumentException("path [" + path + "] is not valid");
            }
            @SuppressWarnings("unchecked")
            List<Object> list = (List<Object>) context;
            int index;
            try {
                index = Integer.parseInt(leafKey);
            } catch (NumberFormatException e) {
                throw new IllegalArgumentException(Errors.notInteger(path, leafKey), e);
            }
            if (index < 0 || index >= list.size()) {
                throw new IllegalArgumentException(Errors.outOfBounds(path, index, list.size()));
            } else {
                if (append) {
                    Object object = list.get(index);
                    Object newList = appendValues(object, value, allowDuplicates, ignoreEmptyValues);
                    if (newList != object) {
                        list.set(index, newList);
                    }
                    return;
                }
                list.set(index, value);
            }
        } else {
            throw new IllegalArgumentException(Errors.cannotSet(path, leafKey, context));
        }
    }

    @SuppressWarnings("unchecked")
    private static Object appendValues(Object maybeList, Object value, boolean allowDuplicates, boolean ignoreEmptyValues) {
        List<Object> list;
        if (maybeList instanceof List) {
            // maybeList is already a list, we append the provided values to it
            list = (List<Object>) maybeList;
        } else {
            // maybeList is a scalar, we convert it to a list and append the provided values to it
            list = new ArrayList<>();
            list.add(maybeList);
        }

        boolean valuesWereAppended = false;
        if (value instanceof List<?> valueList) {
            for (Object val : valueList) {
                if ((allowDuplicates || list.contains(val) == false) && (ignoreEmptyValues == false || valueNotEmpty(val))) {
                    list.add(val);
                    valuesWereAppended = true;
                }
            }
        } else {
            if ((allowDuplicates || list.contains(value) == false) && (ignoreEmptyValues == false || valueNotEmpty(value))) {
                list.add(value);
                valuesWereAppended = true;
            }
        }

        // if no values were appended due to duplication/empties, return the original object so the ingest document remains unmodified
        return valuesWereAppended ? list : maybeList;
    }

    private static boolean valueNotEmpty(Object value) {
        if (value == null) {
            return false;
        }
        if (value instanceof String string) {
            return string.isEmpty() == false;
        }
        return true;
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

    /*
     * This returns the same information as getCtxMap(), but in an unmodifiable map that is safe to send into a script that is
     * not supposed to be modifying the data. If an attempt is made to modify this Map, or a Map, List, or Set nested within it, an
     * UnsupportedOperationException is thrown. If an attempt is made to modify a byte[] within this Map, the attempt succeeds, but the
     * results are not reflected on this IngestDocument. If a user has put any other mutable Object into the IngestDocument, this method
     * makes no attempt to make it immutable. This method just protects users against accidentally modifying the most common types of
     * Objects found in IngestDocuments.
     */
    public Map<String, Object> getUnmodifiableCtxMap() {
        return new UnmodifiableIngestData(ctxMap);
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

    void ensureNoSelfReferences() {
        CollectionUtils.ensureNoSelfReferences(ctxMap.getSource(), "source document");
        CollectionUtils.ensureNoSelfReferences(ingestMetadata, "ingest metadata");
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
     * @return The access pattern of the pipeline that is currently executing on this doc, or
     * {@link IngestPipelineFieldAccessPattern#CLASSIC} if no pipelines are in progress for the sake of backwards compatibility
     */
    public IngestPipelineFieldAccessPattern getAccessPattern() {
        final IngestPipelineFieldAccessPattern current = accessPatternStack.peek();
        return current != null ? current : IngestPipelineFieldAccessPattern.CLASSIC;
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
    @UpdateForV10(owner = UpdateForV10.Owner.DISTRIBUTED)
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

    /**
     * Describes a resolve operation that did not end with a successful result (which is just the object that was found, see
     * {@link #resolve}).
     */
    private record ResolveResult(Object resolvedObject, String errorMessage, String missingFields) {
        /**
         * Due to the access pattern, the resolve operation was only partially completed. The last resolved context object is returned,
         * along with the fields that have been tried up until running into the field limit. This is not an error, as it contains
         * additional information about further resolving the operation.
         * @param lastResolvedObject The last successfully resolved context object from the document
         * @param missingFields The fields from the given path that have not been located yet
         * @return Incomplete result
         */
        static ResolveResult incomplete(Object lastResolvedObject, String missingFields) {
            return new ResolveResult(lastResolvedObject, null, missingFields);
        }

        /**
         * The resolve operation ended with an error. The object at the given path location could not be resolved, either due to it
         * being missing, or the path being invalid.
         * @param errorMessage The error message to be returned.
         * @return Error result
         */
        static ResolveResult error(String errorMessage) {
            return new ResolveResult(null, errorMessage, null);
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

        private static String invalidPath(String fullPath) {
            return "path [" + fullPath + "] is not valid";
        }
    }

    @SuppressWarnings("unchecked")
    private static Object wrapUnmodifiable(Object raw) {
        /*
         * This method makes an attempt to make the raw Object and its children immutable, if it is one of a known set of classes. If raw
         * is a Map, List, or Set, an immutable version will be returned, and an UnsupportedOperationException will be thrown if an attempt
         * to modify it is made. All the Objects in those collections are also made unmodifiable by this method. If raw is a byte[], a copy
         * of the byte[] will be returned so that changes to it will not be reflected in the original data. No exception will be thrown if
         * a user modifies it though.
         */
        if (raw instanceof Map<?, ?> rawMap) {
            return new UnmodifiableIngestData((Map<String, Object>) rawMap);
        } else if (raw instanceof List) {
            return new UnmodifiableIngestList((List<Object>) raw);
        } else if (raw instanceof Set<?> rawSet) {
            return new UnmodifiableIngestSet((Set<Object>) rawSet);
        } else if (raw instanceof byte[] bytes) {
            return bytes.clone();
        }
        return raw;
    }

    private static UnsupportedOperationException unmodifiableException() {
        return new UnsupportedOperationException("Mutating ingest documents in conditionals is not supported");
    }

    private static final class UnmodifiableIngestData implements Map<String, Object> {

        private final Map<String, Object> data;

        UnmodifiableIngestData(Map<String, Object> data) {
            this.data = data;
        }

        @Override
        public int size() {
            return data.size();
        }

        @Override
        public boolean isEmpty() {
            return data.isEmpty();
        }

        @Override
        public boolean containsKey(final Object key) {
            return data.containsKey(key);
        }

        @Override
        public boolean containsValue(final Object value) {
            return data.containsValue(value);
        }

        @Override
        public Object get(final Object key) {
            return wrapUnmodifiable(data.get(key));
        }

        @Override
        public Object put(final String key, final Object value) {
            throw unmodifiableException();
        }

        @Override
        public Object remove(final Object key) {
            throw unmodifiableException();
        }

        @Override
        public void putAll(final Map<? extends String, ?> m) {
            throw unmodifiableException();
        }

        @Override
        public void clear() {
            throw unmodifiableException();
        }

        @Override
        public Set<String> keySet() {
            return Collections.unmodifiableSet(data.keySet());
        }

        @Override
        public Collection<Object> values() {
            return new UnmodifiableIngestList(new ArrayList<>(data.values()));
        }

        @Override
        public Set<Entry<String, Object>> entrySet() {
            return data.entrySet().stream().map(entry -> new Entry<String, Object>() {
                @Override
                public String getKey() {
                    return entry.getKey();
                }

                @Override
                public Object getValue() {
                    return wrapUnmodifiable(entry.getValue());
                }

                @Override
                public Object setValue(final Object value) {
                    throw unmodifiableException();
                }

                @Override
                public boolean equals(final Object o) {
                    return entry.equals(o);
                }

                @Override
                public int hashCode() {
                    return entry.hashCode();
                }
            }).collect(Collectors.toSet());
        }
    }

    private static final class UnmodifiableIngestList implements List<Object> {

        private final List<Object> data;

        UnmodifiableIngestList(List<Object> data) {
            this.data = data;
        }

        @Override
        public int size() {
            return data.size();
        }

        @Override
        public boolean isEmpty() {
            return data.isEmpty();
        }

        @Override
        public boolean contains(final Object o) {
            return data.contains(o);
        }

        @Override
        public Iterator<Object> iterator() {
            return new UnmodifiableIterator(data.iterator());
        }

        @Override
        public Object[] toArray() {
            Object[] wrapped = data.toArray(new Object[0]);
            for (int i = 0; i < wrapped.length; i++) {
                wrapped[i] = wrapUnmodifiable(wrapped[i]);
            }
            return wrapped;
        }

        @Override
        @SuppressWarnings("unchecked")
        public <T> T[] toArray(final T[] a) {
            Object[] raw = data.toArray(new Object[0]);
            T[] wrapped = (T[]) Arrays.copyOf(raw, a.length, a.getClass());
            for (int i = 0; i < wrapped.length; i++) {
                wrapped[i] = (T) wrapUnmodifiable(wrapped[i]);
            }
            return wrapped;
        }

        @Override
        public boolean add(final Object o) {
            throw unmodifiableException();
        }

        @Override
        public boolean remove(final Object o) {
            throw unmodifiableException();
        }

        @Override
        public boolean containsAll(final Collection<?> c) {
            return data.contains(c);
        }

        @Override
        public boolean addAll(final Collection<?> c) {
            throw unmodifiableException();
        }

        @Override
        public boolean addAll(final int index, final Collection<?> c) {
            throw unmodifiableException();
        }

        @Override
        public boolean removeAll(final Collection<?> c) {
            throw unmodifiableException();
        }

        @Override
        public boolean retainAll(final Collection<?> c) {
            throw unmodifiableException();
        }

        @Override
        public void clear() {
            throw unmodifiableException();
        }

        @Override
        public Object get(final int index) {
            return wrapUnmodifiable(data.get(index));
        }

        @Override
        public Object set(final int index, final Object element) {
            throw unmodifiableException();
        }

        @Override
        public void add(final int index, final Object element) {
            throw unmodifiableException();
        }

        @Override
        public Object remove(final int index) {
            throw unmodifiableException();
        }

        @Override
        public int indexOf(final Object o) {
            return data.indexOf(o);
        }

        @Override
        public int lastIndexOf(final Object o) {
            return data.lastIndexOf(o);
        }

        @Override
        public ListIterator<Object> listIterator() {
            return new UnmodifiableListIterator(data.listIterator());
        }

        @Override
        public ListIterator<Object> listIterator(final int index) {
            return new UnmodifiableListIterator(data.listIterator(index));
        }

        @Override
        public List<Object> subList(final int fromIndex, final int toIndex) {
            return new UnmodifiableIngestList(data.subList(fromIndex, toIndex));
        }

        private static final class UnmodifiableListIterator implements ListIterator<Object> {

            private final ListIterator<Object> data;

            UnmodifiableListIterator(ListIterator<Object> data) {
                this.data = data;
            }

            @Override
            public boolean hasNext() {
                return data.hasNext();
            }

            @Override
            public Object next() {
                return wrapUnmodifiable(data.next());
            }

            @Override
            public boolean hasPrevious() {
                return data.hasPrevious();
            }

            @Override
            public Object previous() {
                return wrapUnmodifiable(data.previous());
            }

            @Override
            public int nextIndex() {
                return data.nextIndex();
            }

            @Override
            public int previousIndex() {
                return data.previousIndex();
            }

            @Override
            public void remove() {
                throw unmodifiableException();
            }

            @Override
            public void set(final Object o) {
                throw unmodifiableException();
            }

            @Override
            public void add(final Object o) {
                throw unmodifiableException();
            }
        }
    }

    private static final class UnmodifiableIngestSet implements Set<Object> {
        private final Set<Object> data;

        UnmodifiableIngestSet(Set<Object> data) {
            this.data = data;
        }

        @Override
        public int size() {
            return data.size();
        }

        @Override
        public boolean isEmpty() {
            return data.isEmpty();
        }

        @Override
        public boolean contains(Object o) {
            return data.contains(o);
        }

        @Override
        public Iterator<Object> iterator() {
            return new UnmodifiableIterator(data.iterator());
        }

        @Override
        public Object[] toArray() {
            return data.toArray();
        }

        @Override
        public <T> T[] toArray(T[] a) {
            return data.toArray(a);
        }

        @Override
        public boolean add(Object o) {
            throw unmodifiableException();
        }

        @Override
        public boolean remove(Object o) {
            throw unmodifiableException();
        }

        @Override
        public boolean containsAll(Collection<?> c) {
            return data.containsAll(c);
        }

        @Override
        public boolean addAll(Collection<?> c) {
            throw unmodifiableException();
        }

        @Override
        public boolean retainAll(Collection<?> c) {
            throw unmodifiableException();
        }

        @Override
        public boolean removeAll(Collection<?> c) {
            throw unmodifiableException();
        }

        @Override
        public void clear() {
            throw unmodifiableException();
        }
    }

    private static final class UnmodifiableIterator implements Iterator<Object> {
        private final Iterator<Object> it;

        UnmodifiableIterator(Iterator<Object> it) {
            this.it = it;
        }

        @Override
        public boolean hasNext() {
            return it.hasNext();
        }

        @Override
        public Object next() {
            return wrapUnmodifiable(it.next());
        }

        @Override
        public void remove() {
            throw unmodifiableException();
        }
    }
}
