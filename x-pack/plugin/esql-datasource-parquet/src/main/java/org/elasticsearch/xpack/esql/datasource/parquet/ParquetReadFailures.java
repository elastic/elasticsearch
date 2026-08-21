/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasource.parquet;

import org.elasticsearch.xpack.esql.datasources.ExternalFailures;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ExecutionException;

/**
 * Maps a failure on the Parquet read path to the exception the iterator should throw, without
 * recasting I/O or typed Elasticsearch failures as {@link IllegalArgumentException}.
 * <p>
 * Blanket IAE recasts made every storage fault look like bad input (HTTP 400) and hid
 * {@code ExternalUnavailableException} (503) and bug-class {@code IllegalStateException} (500)
 * from {@link ExternalFailures#classify}. Callers replace {@code catch (Exception) → new IAE}
 * with {@code throw ParquetReadFailures.wrap(t, context)}. {@code Future.join()}/{@code get()}
 * wrap the raw cause in {@link java.util.concurrent.CompletionException}/
 * {@link java.util.concurrent.ExecutionException}; those are peeled here because
 * {@code ExceptionsHelper.unwrapCause} does not.
 * <p>
 * {@code context} is applied to I/O and to {@link IllegalArgumentException} (malformed pages)
 * so call-site column/file strings show up in the 400 message. Other {@link RuntimeException}s
 * (typed ES failures, {@code ParquetDecodingException}, bug-class ISE) pass through so
 * {@link ExternalFailures#classify} still sees the original type.
 */
final class ParquetReadFailures {

    private ParquetReadFailures() {}

    static RuntimeException wrap(Throwable t, String context) {
        Throwable cause = unwrapJoin(t);
        if (cause instanceof Error error) {
            throw error;
        }
        if (cause instanceof IOException || cause instanceof UncheckedIOException) {
            return ExternalFailures.surface(cause, context);
        }
        if (cause instanceof IllegalArgumentException iae) {
            String detail = iae.getMessage();
            if (detail == null || detail.isEmpty()) {
                detail = iae.getClass().getSimpleName();
            }
            return new IllegalArgumentException(context + ": " + detail, iae);
        }
        if (cause instanceof RuntimeException re) {
            return re;
        }
        return ExternalFailures.surface(cause, context);
    }

    private static Throwable unwrapJoin(Throwable t) {
        Throwable current = t;
        while (current instanceof CompletionException || current instanceof ExecutionException) {
            Throwable next = current.getCause();
            if (next == null || next == current) {
                break;
            }
            current = next;
        }
        return current;
    }
}
