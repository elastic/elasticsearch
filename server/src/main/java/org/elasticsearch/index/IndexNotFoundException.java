/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */
package org.elasticsearch.index;

import org.elasticsearch.ResourceNotFoundException;
import org.elasticsearch.cluster.metadata.ProjectId;
import org.elasticsearch.common.io.stream.StreamInput;

import java.io.IOException;

/**
 * Exception thrown when a requested index cannot be found in the cluster.
 * This exception is used to indicate that an operation failed because the specified index does not exist.
 */
public final class IndexNotFoundException extends ResourceNotFoundException {
    /**
     * Constructs an IndexNotFoundException with a custom message and index name.
     * The final message will be formatted as "no such index [indexName] and customMessage".
     *
     * @param message additional context message to append to the standard error message
     * @param index the name of the index that was not found
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * throw new IndexNotFoundException("it was deleted", "my-index");
     * // Message: "no such index [my-index] and it was deleted"
     * }</pre>
     */
    public IndexNotFoundException(String message, String index) {
        super("no such index [" + index + "] and " + message);
        setIndex(index);

    }

    /**
     * Constructs an IndexNotFoundException with a custom message and Index object.
     * The final message will be formatted as "no such index [indexName] and customMessage".
     *
     * @param message additional context message to append to the standard error message
     * @param index the Index object that was not found
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * Index index = new Index("my-index", "abc123");
     * throw new IndexNotFoundException("it was deleted", index);
     * }</pre>
     */
    public IndexNotFoundException(String message, Index index) {
        super("no such index [" + index + "] and " + message);
        setIndex(index);
    }

    /**
     * Constructs an IndexNotFoundException with just an index name.
     *
     * @param index the name of the index that was not found
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * throw new IndexNotFoundException("my-index");
     * // Message: "no such index [my-index]"
     * }</pre>
     */
    public IndexNotFoundException(String index) {
        this(index, (Throwable) null);
    }

    /**
     * Constructs an IndexNotFoundException with an index name and a cause.
     *
     * @param index the name of the index that was not found
     * @param cause the underlying cause of this exception (may be null)
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * try {
     *     // some operation
     * } catch (IOException e) {
     *     throw new IndexNotFoundException("my-index", e);
     * }
     * }</pre>
     */
    public IndexNotFoundException(String index, Throwable cause) {
        super("no such index [" + index + "]", cause);
        setIndex(index);
    }

    /**
     * Constructs an IndexNotFoundException for an index within a specific project.
     * The message will indicate both the index and the project that were searched.
     *
     * @param index the Index object that was not found
     * @param id the project identifier where the index was searched
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * Index index = new Index("my-index", "abc123");
     * ProjectId projectId = new ProjectId("project-1");
     * throw new IndexNotFoundException(index, projectId);
     * // Message: "no such index [my-index] in project [project-1]"
     * }</pre>
     */
    public IndexNotFoundException(Index index, ProjectId id) {
        super("no such index [" + index.getName() + "] in project [" + id + "]");
        setIndex(index);
    }

    /**
     * Constructs an IndexNotFoundException with an Index object.
     *
     * @param index the Index object that was not found
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * Index index = new Index("my-index", "abc123");
     * throw new IndexNotFoundException(index);
     * }</pre>
     */
    public IndexNotFoundException(Index index) {
        this(index, (Throwable) null);
    }

    /**
     * Constructs an IndexNotFoundException with an Index object and a cause.
     *
     * @param index the Index object that was not found
     * @param cause the underlying cause of this exception (may be null)
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * Index index = new Index("my-index", "abc123");
     * try {
     *     // some operation
     * } catch (IOException e) {
     *     throw new IndexNotFoundException(index, e);
     * }
     * }</pre>
     */
    public IndexNotFoundException(Index index, Throwable cause) {
        super("no such index [" + index.getName() + "]", cause);
        setIndex(index);
    }

    /**
     * Deserializes an IndexNotFoundException from a stream input.
     *
     * @param in the stream to read from
     * @throws IOException if an I/O error occurs while reading from the stream
     */
    public IndexNotFoundException(StreamInput in) throws IOException {
        super(in);
    }

}
