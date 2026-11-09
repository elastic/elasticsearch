/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.spill.spi;

import org.elasticsearch.action.ActionListener;

/**
 * An immutable, finished object. Opaque to the engine except for its size; any number of readers may be open at once.
 */
public interface SpillObject {

    long sizeInBytes();

    /**
     * A reader over {@code [offset, offset + length)}. The range is validated before anything is allocated: a listener
     * failure with {@link IllegalArgumentException} means nothing was opened.
     */
    void openRange(long offset, long length, ActionListener<SpillReader> listener);

    /**
     * Deletes the object; idempotent, and the scope's close does it too. A read issued afterwards fails with
     * {@link IllegalStateException}; a read outstanding at the delete completes either way. The bytes return to the quota
     * when the listener completes.
     */
    void delete(ActionListener<Void> listener);
}
