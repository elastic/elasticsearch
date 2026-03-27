/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.common.blobstore;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.common.BackoffPolicy;
import org.elasticsearch.common.blobstore.support.TenaciousRetryBlobContainer;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.core.CheckedConsumer;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.repositories.RepositoriesMetrics;
import org.elasticsearch.telemetry.RecordingMeterRegistry;
import org.elasticsearch.test.ESTestCase;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;

import static org.hamcrest.Matchers.is;
import static org.mockito.Mockito.clearInvocations;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockingDetails;

public class TenaciousRetryBlobContainerTests extends ESTestCase {

    public void testOnlyRetryListBlobs() throws IOException {
        final var recordingMeterRegistry = new RecordingMeterRegistry();
        final var repositoriesMetrics = new RepositoriesMetrics(recordingMeterRegistry);

        BlobContainer blobContainer = mock(BlobContainer.class, invocationOnMock -> {
            assert invocationOnMock.getMethod().getName().equals("listBlobs") == false;
            assert invocationOnMock.getMethod().getName().equals("listBlobsByPrefix") == false;
            // Not supported
            assert invocationOnMock.getMethod().getName().equals("copyBlob") == false;
            throw new IOException(invocationOnMock.getMethod().getName());
        });

        TenaciousRetryBlobContainer tenaciousRetryBlobContainer = new TenaciousRetryBlobContainer(
            blobContainer,
            Integer.MAX_VALUE,
            BackoffPolicy.linearBackoff(TimeValue.timeValueMillis(50), Integer.MAX_VALUE, TimeValue.ONE_MINUTE),
            repositoriesMetrics
        ) {

            @Override
            protected boolean isExceptionRetryable(Exception e) {
                return true;
            }

            @Override
            protected String getRepositoryType() {
                return "test";
            }

            @Override
            protected BlobContainer wrapChild(BlobContainer child) {
                return child;
            }
        };

        List<Method> nonRetryable = nonRetryableBlobContainerMethods();

        for (int i = 0; i < randomIntBetween(50, 100); i++) {
            Method method = randomFrom(nonRetryable);
            Object[] args = buildArgs(method);
            System.out.println(method.getName());
            InvocationTargetException ex = expectThrows(
                InvocationTargetException.class,
                () -> method.invoke(tenaciousRetryBlobContainer, args)
            );
            assertThat(ex.getTargetException().getMessage(), is(method.getName()));

            long matchingCalls = mockingDetails(blobContainer).getInvocations()
                .stream()
                .filter(inv -> inv.getMethod().getName().equals(method.getName()))
                .count();

            assertThat(matchingCalls, is(1L));
            clearInvocations(blobContainer);
        }
    }

    private Object[] buildArgs(Method method) {
        Class<?>[] ptypes = method.getParameterTypes();
        Object[] args = new Object[ptypes.length];
        for (int i = 0; i < ptypes.length; i++) {
            args[i] = sampleArgument(ptypes[i]);
        }
        return args;
    }

    private List<Method> nonRetryableBlobContainerMethods() {
        List<Method> methods = new ArrayList<>();
        for (Method m : BlobContainer.class.getMethods()) {
            // Retryable methods.
            if (m.getName().equals("listBlobs") || m.getName().equals("listBlobsByPrefix")) {
                continue;
            }
            // Not supported
            if (m.getName().equals("copyBlob")) {
                continue;
            }
            // Static methods
            if (Modifier.isStatic(m.getModifiers())) {
                continue;
            }

            methods.add(m);
        }
        return Collections.unmodifiableList(methods);
    }

    private Object sampleArgument(Class<?> type) {
        if (type == OperationPurpose.class) {
            OperationPurpose[] values = OperationPurpose.values();
            return randomFrom(values);
        }
        if (type == String.class) {
            return randomIdentifier();
        }
        if (type == long.class || type == Long.class) {
            return randomLong();
        }
        if (type == boolean.class || type == Boolean.class) {
            return randomBoolean();
        }
        if (type == InputStream.class) {
            return InputStream.nullInputStream();
        }
        if (type == BlobContainer.class) {
            return mock(BlobContainer.class);
        }
        if (type == Iterator.class) {
            return Collections.emptyIterator();
        }
        if (type == BytesReference.class) {
            return BytesArray.EMPTY;
        }
        if (type == ActionListener.class) {
            return ActionListener.noop();
        }
        if (type == CheckedConsumer.class) {
            return (CheckedConsumer<OutputStream, IOException>) outputStream -> {};
        }
        if (type == BlobContainer.BlobMultiPartInputStreamProvider.class) {
            return (BlobContainer.BlobMultiPartInputStreamProvider) (offset, length) -> new ByteArrayInputStream(
                randomByteArrayOfLength(10)
            );
        }
        throw new IllegalArgumentException("Add a sample for parameter type [" + type.getName() + "] used by BlobContainer");
    }

}
