/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.monitoring.exporter.local;

import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.core.LogEvent;
import org.elasticsearch.ElasticsearchWrapperException;
import org.elasticsearch.action.DocWriteRequest;
import org.elasticsearch.action.bulk.BulkItemResponse;
import org.elasticsearch.action.support.PlainActionFuture;
import org.elasticsearch.common.time.DateFormatter;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.MockLog;
import org.elasticsearch.test.client.NoOpClient;
import org.elasticsearch.xpack.monitoring.exporter.ExportException;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.stream.IntStream;
import java.util.stream.StreamSupport;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.instanceOf;

public class LocalBulkTests extends ESTestCase {

    private static final Logger bulkLogger = LogManager.getLogger(LocalBulk.class);

    public void testLogsAllFailuresWhenAtOrBelowLimit() throws Exception {
        final int failures = randomIntBetween(1, 10);
        final List<String> warnings = throwExportException(failures);
        assertThat(warnings, hasSize(failures));
        warnings.forEach(message -> assertThat(message, containsString("unexpected error while indexing monitoring document")));
    }

    public void testLimitsLoggedFailuresAndLogsSummary() throws Exception {
        final int failures = randomIntBetween(11, 50);
        final List<String> warnings = throwExportException(failures);
        assertThat(warnings, hasSize(11));
        warnings.subList(0, 10).forEach(message -> assertThat(message, containsString("unexpected error while indexing")));
        assertThat(warnings.get(10), equalTo(failures + " exceptions occurred, skipped logging " + (failures - 10) + " of them"));
    }

    public void testLogsUniqueExceptionTypesAfterLimitIsExceeded() throws Exception {
        final int duplicates = randomIntBetween(10, 30);
        final List<Exception> causes = new ArrayList<>();
        IntStream.range(0, duplicates).forEach(i -> causes.add(new IllegalStateException("failure-" + i)));
        causes.add(new IllegalArgumentException("unique-1"));
        causes.add(new IllegalStateException("duplicate-after-unique"));
        causes.add(new UnsupportedOperationException("unique-2"));

        final List<String> warnings = throwExportException(causes);

        // 10 within the limit, then the two unique types past it, then the summary
        final int total = duplicates + 3;
        assertThat(warnings, hasSize(13));
        assertThat(warnings.get(12), equalTo(total + " exceptions occurred, skipped logging " + (total - 12) + " of them"));
    }

    /**
     * Fails a bulk with {@code failures} failed items, returning the WARN messages logged and asserting that the listener receives
     * every failure regardless of how many were logged.
     */
    private List<String> throwExportException(int failures) throws Exception {
        return throwExportException(
            IntStream.range(0, failures).<Exception>mapToObj(i -> new IllegalStateException("failure-" + i)).toList()
        );
    }

    private List<String> throwExportException(List<Exception> causes) throws Exception {
        final int failures = causes.size();
        final List<String> warnings = new ArrayList<>();
        final BulkItemResponse[] items = IntStream.range(0, failures)
            .mapToObj(
                i -> BulkItemResponse.failure(
                    i,
                    DocWriteRequest.OpType.INDEX,
                    new BulkItemResponse.Failure(
                        "index",
                        "id-" + i,
                        randomBoolean() ? causes.get(i) : new TestWrappedException(causes.get(i))
                    )
                )
            )
            .toArray(BulkItemResponse[]::new);

        try (var threadPool = createThreadPool(); var mockLog = MockLog.capture(LocalBulk.class)) {
            mockLog.addExpectation(new MockLog.LoggingExpectation() {
                @Override
                public void match(LogEvent event) {
                    if (event.getLevel() == Level.WARN) {
                        warnings.add(event.getMessage().getFormattedMessage());
                    }
                }

                @Override
                public void assertMatched() {}
            });

            final var bulk = new LocalBulk("test", bulkLogger, new NoOpClient(threadPool), DateFormatter.forPattern("yyyy.MM.dd"));
            final PlainActionFuture<Void> listener = new PlainActionFuture<>();
            bulk.throwExportException(items, listener);

            final var e = expectThrows(ExecutionException.class, listener::get);
            assertThat(e.getCause(), instanceOf(ExportException.class));
            assertThat(StreamSupport.stream(((ExportException) e.getCause()).spliterator(), false).count(), equalTo((long) failures));
            mockLog.assertAllExpectationsMatched();
        }
        return warnings;
    }

    private static class TestWrappedException extends RuntimeException implements ElasticsearchWrapperException {
        TestWrappedException(Throwable cause) {
            super(cause);
        }
    }
}
