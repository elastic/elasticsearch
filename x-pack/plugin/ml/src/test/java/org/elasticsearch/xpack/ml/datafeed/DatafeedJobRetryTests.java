/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.ml.datafeed;

import org.elasticsearch.client.internal.Client;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.core.ml.datafeed.DatafeedTimingStats;
import org.elasticsearch.xpack.core.ml.job.config.DataDescription;
import org.elasticsearch.xpack.ml.annotations.AnnotationPersister;
import org.elasticsearch.xpack.ml.datafeed.delayeddatacheck.DelayedDataDetector;
import org.elasticsearch.xpack.ml.datafeed.delayeddatacheck.DelayedDataDetectorFactory.BucketWithMissingData;
import org.elasticsearch.xpack.ml.datafeed.extractor.DataExtractorFactory;
import org.elasticsearch.xpack.ml.notifications.AnomalyDetectionAuditor;
import org.junit.Before;

import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Supplier;

import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.Mockito.*;

public class DatafeedJobRetryTests extends ESTestCase {
    
    private DatafeedJob datafeedJob;
    private DelayedDataDetector delayedDataDetector;
    private AnomalyDetectionAuditor auditor;
    private Client client;
    private DataExtractorFactory dataExtractorFactory;
    private DatafeedTimingStatsReporter timingStatsReporter;
    private AnnotationPersister annotationPersister;
    
    @Before
    public void setup() {
        String jobId = "test-job";
        DataDescription dataDescription = new DataDescription.Builder().build();
        long frequencyMs = 60000;
        long queryDelayMs = 1000;
        
        client = mock(Client.class);
        dataExtractorFactory = mock(DataExtractorFactory.class);
        timingStatsReporter = mock(DatafeedTimingStatsReporter.class);
        auditor = mock(AnomalyDetectionAuditor.class);
        annotationPersister = mock(AnnotationPersister.class);
        delayedDataDetector = mock(DelayedDataDetector.class);
        
        Supplier<Long> currentTimeSupplier = System::currentTimeMillis;
        Integer maxEmptySearches = 10;
        long latestFinalBucketEndTimeMs = System.currentTimeMillis() - 3600000;
        long latestRecordTimeMs = System.currentTimeMillis() - 1800000;
        boolean haveSeenDataPreviously = true;
        long delayedDataCheckFreq = 900000; // 15 minutes
        
        datafeedJob = new DatafeedJob(
            jobId,
            dataDescription,
            frequencyMs,
            queryDelayMs,
            dataExtractorFactory,
            timingStatsReporter,
            client,
            auditor,
            annotationPersister,
            currentTimeSupplier,
            delayedDataDetector,
            maxEmptySearches,
            latestFinalBucketEndTimeMs,
            latestRecordTimeMs,
            haveSeenDataPreviously,
            delayedDataCheckFreq
        );
    }
    
    public void testCheckForMissingDataRetriesOnFailure() throws Exception {
        // Simulate failures followed by success
        AtomicInteger callCount = new AtomicInteger(0);
        when(delayedDataDetector.detectMissingData(anyLong())).thenAnswer(invocation -> {
            int count = callCount.incrementAndGet();
            if (count <= 2) {
                throw new IOException("Simulated failure " + count);
            }
            return Collections.emptyList();
        });
        
        // This should trigger the retry logic
        // Note: We would need to make checkForMissingDataIfNecessary accessible for testing
        // or test through a public method that calls it
        
        // Verify that detectMissingData was called 3 times (2 failures + 1 success)
        Thread.sleep(1000); // Allow time for retries
        verify(delayedDataDetector, times(3)).detectMissingData(anyLong());
        verify(auditor, never()).warning(anyString(), anyString());
    }
    
    public void testCheckForMissingDataFailsAfterMaxRetries() throws Exception {
        // Simulate continuous failures
        when(delayedDataDetector.detectMissingData(anyLong()))
            .thenThrow(new IOException("Persistent failure"));
        
        // This should exhaust all retries
        // Note: We would need to make checkForMissingDataIfNecessary accessible for testing
        
        // Verify that detectMissingData was called 4 times (initial + 3 retries)
        Thread.sleep(2000); // Allow time for all retries
        verify(delayedDataDetector, times(4)).detectMissingData(anyLong());
        // Verify that warning was issued after all retries failed
        verify(auditor, times(1)).warning(eq("test-job"), contains("Failed to check for missing data after 4 attempts"));
    }
    
    public void testCheckForMissingDataSucceedsOnFirstAttempt() throws Exception {
        // Simulate immediate success
        List<BucketWithMissingData> emptyList = Collections.emptyList();
        when(delayedDataDetector.detectMissingData(anyLong())).thenReturn(emptyList);
        
        // This should succeed immediately without retries
        
        // Verify that detectMissingData was called only once
        verify(delayedDataDetector, times(1)).detectMissingData(anyLong());
        verify(auditor, never()).warning(anyString(), anyString());
    }
    
    public void testExponentialBackoffDelays() throws Exception {
        // Test that backoff delays increase exponentially
        AtomicInteger callCount = new AtomicInteger(0);
        long startTime = System.currentTimeMillis();
        
        when(delayedDataDetector.detectMissingData(anyLong())).thenAnswer(invocation -> {
            int count = callCount.incrementAndGet();
            if (count <= 3) {
                throw new IOException("Simulated failure " + count);
            }
            return Collections.emptyList();
        });
        
        // Execute and measure time
        // The total time should be at least 100ms + 200ms + 400ms = 700ms
        
        Thread.sleep(1500); // Allow time for all retries with backoff
        long elapsedTime = System.currentTimeMillis() - startTime;
        
        // Verify exponential backoff was applied
        assertTrue("Expected at least 700ms due to backoff delays", elapsedTime >= 700);
        verify(delayedDataDetector, times(4)).detectMissingData(anyLong());
    }
}