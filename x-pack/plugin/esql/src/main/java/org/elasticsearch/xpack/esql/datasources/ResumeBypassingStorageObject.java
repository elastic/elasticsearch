/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasources;

import org.elasticsearch.xpack.esql.datasources.spi.StorageObject;

import java.io.InputStream;
import java.util.function.Function;

/**
 * Implemented by this package's {@link StorageObject} decorators so a best-effort read during release can reach
 * the provider stream without going through {@link RetryableStorageObject}'s resume. The end-of-body read
 * {@link DecompressingStorageObject} does so S3 can pool the connection is one: a fault there must fall through
 * to the abort that follows, not sleep through a backoff and re-open a GET inside {@code close()}. The routing
 * mirrors {@link StorageObject#abortStream(InputStream)}: each layer unwraps its own stream and hands the inner
 * one to its delegate, so the bypass works whatever order the decorators are stacked in.
 */
interface ResumeBypassingStorageObject {

    /**
     * Returns a stream that reads the rest of {@code stream}, an instance this object returned, without resuming
     * on a fault. Returns {@code stream} unchanged if it did not come from this object.
     */
    InputStream withoutResume(InputStream stream);

    /** {@link #withoutResume(InputStream)} on {@code owner} if it is a decorator from this package, else {@code stream}. */
    static InputStream withoutResume(StorageObject owner, InputStream stream) {
        return owner instanceof ResumeBypassingStorageObject bypassing ? bypassing.withoutResume(stream) : stream;
    }

    /**
     * For decorators that wrap the provider stream in a single wrapper of their own: if {@code stream} is a
     * {@code wrapperType}, unwraps it with {@code inner} and continues on {@code delegate}; else returns {@code stream}
     * unchanged because it did not come from this decorator.
     */
    static <W extends InputStream> InputStream withoutResumeThrough(
        StorageObject delegate,
        InputStream stream,
        Class<W> wrapperType,
        Function<W, InputStream> inner
    ) {
        return wrapperType.isInstance(stream) ? withoutResume(delegate, inner.apply(wrapperType.cast(stream))) : stream;
    }
}
