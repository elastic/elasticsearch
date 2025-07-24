/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.codec.vectors.reflect;

import org.apache.lucene.codecs.KnnVectorsReader;
import org.elasticsearch.core.SuppressForbidden;

import java.lang.invoke.MethodHandle;
import java.lang.invoke.MethodHandles;

/**
 * Reflective access to unwrap non-accessible delegate in AssertingKnnVectorsReader.
 * Remove once KnnVectorsReaders::getOffHeapByteSize is available.
 */
public class AssertingKnnVectorsReaderReflect {

    @SuppressForbidden(reason = "static type is not accessible")
    public static KnnVectorsReader unwrapAssertingReader(KnnVectorsReader reader) {
        try {
            if (ASSERTING_ASSERT_KNN_READER_CLS != null && ASSERTING_ASSERT_KNN_READER_CLS.isAssignableFrom(reader.getClass())) {
                return (KnnVectorsReader) GET_VECTOR_INDEX_LENGTH_HANDLE.invoke(reader);
            }
        } catch (Throwable t) {
            handleThrowable(t);
        }
        return reader;
    }

    private static final Class<?> ASSERTING_ASSERT_KNN_READER_CLS = getAssertingReaderOrNull();
    private static final MethodHandle GET_VECTOR_INDEX_LENGTH_HANDLE = getDelegateFieldHandle();

    private static Class<?> getAssertingReaderOrNull() {
        try {
            return Class.forName("org.apache.lucene.tests.codecs.asserting.AssertingKnnVectorsFormat$AssertingKnnVectorsReader");
        } catch (ClassNotFoundException e) {
            return null;
        }
    }

    private static MethodHandle getDelegateFieldHandle() {
        try {
            var cls = getAssertingReaderOrNull();
            if (cls == null) {
                return MethodHandles.throwException(KnnVectorsReader.class, AssertionError.class);
            }
            var lookup = MethodHandles.privateLookupIn(cls, MethodHandles.lookup());
            return lookup.findGetter(cls, "delegate", KnnVectorsReader.class);
        } catch (ReflectiveOperationException e) {
            throw new AssertionError(e);
        }
    }

    static void handleThrowable(Throwable t) {
        if (t instanceof Error error) {
            throw error;
        } else if (t instanceof RuntimeException runtimeException) {
            throw runtimeException;
        } else {
            throw new AssertionError(t);
        }
    }
}
