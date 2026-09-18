/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.common.blobstore;

import org.elasticsearch.common.blobstore.support.BlobMetadata;
import org.elasticsearch.common.blobstore.support.TenaciousRetryBlobContainer;
import org.elasticsearch.repositories.RepositoriesMetrics;
import org.elasticsearch.telemetry.InstrumentType;
import org.elasticsearch.telemetry.RecordingMeterRegistry;
import org.elasticsearch.test.ESTestCase;

import java.io.IOException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

import static org.elasticsearch.repositories.RepositoriesMetrics.METRIC_TRANSIENT_ERROR_RETRY_ATTEMPTS_TOTAL;
import static org.elasticsearch.repositories.RepositoriesMetrics.METRIC_TRANSIENT_ERROR_RETRY_SUCCESS_TOTAL;
import static org.elasticsearch.repositories.RepositoriesMetrics.METRIC_TRANSIENT_ERROR_RETRY_TOTAL;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.is;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.clearInvocations;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.reset;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class TenaciousRetryBlobContainerTests extends ESTestCase {

    public void testRetryListOperations() throws IOException {
        final var recordingMeterRegistry = new RecordingMeterRegistry();
        final var repositoriesMetrics = new RepositoriesMetrics(recordingMeterRegistry);

        final Map<String, BlobMetadata> answer = new HashMap<>();
        final Map<String, BlobContainer> children = new HashMap<>();
        BlobContainer blobContainer = mock(BlobContainer.class);
        BlobPath blobPath = mock(BlobPath.class);
        when(blobPath.buildAsString()).thenReturn("/tenacious/retries");
        when(blobContainer.listBlobs(any())).thenThrow(new IOException("listBlobs"));
        when(blobContainer.listBlobsByPrefix(any(), any())).thenThrow(new IOException());
        when(blobContainer.children(any())).thenThrow(new IOException("children"));
        when(blobContainer.path()).thenReturn(blobPath);

        TenaciousRetryBlobContainer tenaciousRetryBlobContainer = new TenaciousRetryBlobContainer(blobContainer, repositoriesMetrics) {
            @Override
            protected boolean isExceptionRetryable(Exception e) {
                return e instanceof IOException;
            }

            @Override
            protected Map<String, Object> getMetricsAttributes(RetryMethod method, OperationPurpose operationPurpose) {
                return Map.of("repo_type", "test", "operation_purpose", operationPurpose.getKey());
            }

            @Override
            protected BlobContainer wrapChild(BlobContainer child) {
                return child;
            }
        };

        var nonIndicesPurposes = Arrays.stream(OperationPurpose.values())
            .filter(operationPurpose -> operationPurpose != OperationPurpose.INDICES)
            .toList();

        // No retry for non indices purposes
        for (int i = 0; i < randomInt(nonIndicesPurposes.size()); i++) {
            expectThrows(IOException.class, () -> tenaciousRetryBlobContainer.listBlobs(randomFrom(nonIndicesPurposes)));

            expectThrows(
                IOException.class,
                () -> tenaciousRetryBlobContainer.listBlobsByPrefix(randomFrom(nonIndicesPurposes), randomIndexName())
            );

            expectThrows(IOException.class, () -> tenaciousRetryBlobContainer.children(randomFrom((nonIndicesPurposes))));

            verify(blobContainer, times(1)).listBlobs(any());
            verify(blobContainer, times(1)).listBlobsByPrefix(any(), any());
            verify(blobContainer, times(1)).children(any());
            clearInvocations(blobContainer);
        }

        reset(blobContainer);
        when(blobContainer.listBlobs(any())).thenThrow(new IOException("listBlobs"))
            .thenThrow(new IOException("listBlobs"))
            .thenReturn(answer);
        when(blobContainer.listBlobsByPrefix(any(), any())).thenThrow(new IOException())
            .thenThrow(new IOException())
            .thenThrow(new IllegalArgumentException());
        when(blobContainer.path()).thenReturn(blobPath);

        assertThat(tenaciousRetryBlobContainer.listBlobs(OperationPurpose.INDICES), is(answer));
        verify(blobContainer, times(3)).listBlobs(any());
        recordingMeterRegistry.getRecorder().collect();

        assertThat(getMeasurements(recordingMeterRegistry, METRIC_TRANSIENT_ERROR_RETRY_ATTEMPTS_TOTAL), equalTo(2));
        assertThat(getMeasurements(recordingMeterRegistry, METRIC_TRANSIENT_ERROR_RETRY_SUCCESS_TOTAL), equalTo(1));

        recordingMeterRegistry.getRecorder().resetCalls();
        expectThrows(
            IllegalArgumentException.class,
            () -> tenaciousRetryBlobContainer.listBlobsByPrefix(OperationPurpose.INDICES, randomIndexName())
        );
        recordingMeterRegistry.getRecorder().collect();
        assertThat(getMeasurements(recordingMeterRegistry, METRIC_TRANSIENT_ERROR_RETRY_ATTEMPTS_TOTAL), equalTo(2));
        assertThat(getMeasurements(recordingMeterRegistry, METRIC_TRANSIENT_ERROR_RETRY_TOTAL), equalTo(1));
        assertThat(getAttributes(recordingMeterRegistry, METRIC_TRANSIENT_ERROR_RETRY_ATTEMPTS_TOTAL).size(), equalTo(2));
        assertThat(getAttributes(recordingMeterRegistry, METRIC_TRANSIENT_ERROR_RETRY_ATTEMPTS_TOTAL).get("repo_type"), equalTo("test"));
        assertThat(
            getAttributes(recordingMeterRegistry, METRIC_TRANSIENT_ERROR_RETRY_ATTEMPTS_TOTAL).get("operation_purpose"),
            equalTo(OperationPurpose.INDICES.getKey())
        );

        // Key retryable method children()
        reset(blobContainer);
        recordingMeterRegistry.getRecorder().resetCalls();
        when(blobContainer.children(any())).thenThrow(new IOException("children"))
            .thenThrow(new IOException("children"))
            .thenReturn(children);
        when(blobContainer.path()).thenReturn(blobPath);

        assertThat(tenaciousRetryBlobContainer.children(OperationPurpose.INDICES), is(children));
        verify(blobContainer, times(3)).children(any());
        recordingMeterRegistry.getRecorder().collect();

        assertThat(getMeasurements(recordingMeterRegistry, METRIC_TRANSIENT_ERROR_RETRY_ATTEMPTS_TOTAL), equalTo(2));
        assertThat(getMeasurements(recordingMeterRegistry, METRIC_TRANSIENT_ERROR_RETRY_SUCCESS_TOTAL), equalTo(1));

        reset(blobContainer);
        recordingMeterRegistry.getRecorder().resetCalls();
        when(blobContainer.children(any())).thenThrow(new IOException("children"))
            .thenThrow(new IOException("children"))
            .thenThrow(new IOException("children"))
            .thenThrow(new IllegalArgumentException());
        when(blobContainer.path()).thenReturn(blobPath);
        Throwable ex = expectThrows(IllegalArgumentException.class, () -> tenaciousRetryBlobContainer.children(OperationPurpose.INDICES));
        assertThat(Arrays.stream(ex.getSuppressed()).count(), is(3L));
        assertThat(Arrays.stream(ex.getSuppressed()).findFirst().orElseThrow(), instanceOf(IOException.class));
        recordingMeterRegistry.getRecorder().collect();

        assertThat(getMeasurements(recordingMeterRegistry, METRIC_TRANSIENT_ERROR_RETRY_ATTEMPTS_TOTAL), equalTo(3));
        assertThat(getMeasurements(recordingMeterRegistry, METRIC_TRANSIENT_ERROR_RETRY_TOTAL), equalTo(1));
        assertThat(getAttributes(recordingMeterRegistry, METRIC_TRANSIENT_ERROR_RETRY_TOTAL).get("repo_type"), equalTo("test"));
        assertThat(
            getAttributes(recordingMeterRegistry, METRIC_TRANSIENT_ERROR_RETRY_TOTAL).get("operation_purpose"),
            equalTo(OperationPurpose.INDICES.getKey())
        );
    }

    private int getMeasurements(RecordingMeterRegistry meterRegistry, String name) {
        return meterRegistry.getRecorder().getMeasurements(InstrumentType.LONG_COUNTER, name).size();
    }

    private Map<String, Object> getAttributes(RecordingMeterRegistry meterRegistry, String name) {
        return meterRegistry.getRecorder().getMeasurements(InstrumentType.LONG_COUNTER, name).getFirst().attributes();
    }
}
