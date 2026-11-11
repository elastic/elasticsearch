/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.plugin;

import org.elasticsearch.TransportVersion;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.PlainActionFuture;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.compute.operator.Driver;
import org.elasticsearch.compute.operator.DriverCompletionInfo;
import org.elasticsearch.compute.operator.DriverContext;
import org.elasticsearch.compute.operator.Operator;
import org.elasticsearch.compute.test.CannedSourceOperator;
import org.elasticsearch.compute.test.ComputeTestCase;
import org.elasticsearch.compute.test.TestDriverFactory;
import org.elasticsearch.compute.test.TestResultPageSinkOperator;
import org.elasticsearch.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.sameInstance;

/**
 * What a failed compute tells its listener about the work its drivers did, see {@link ComputeCompletionListener}.
 */
public class FailedComputeUsageTests extends ComputeTestCase {

    private record UsageStatus(long bytesRead, long readNanos, long readCpuNanos) implements Operator.Status {
        @Override
        public String getWriteableName() {
            return "usage_status";
        }

        @Override
        public TransportVersion getMinimalSupportedVersion() {
            return TransportVersion.current();
        }

        @Override
        public void writeTo(StreamOutput out) {
            throw new UnsupportedOperationException("not serialized in this test");
        }

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            return builder.startObject().field("bytes_read", bytesRead).endObject();
        }
    }

    /** Stands for an external source operator: it reports what it read. */
    private static class ReadingSourceOperator extends CannedSourceOperator {
        private final UsageStatus status;

        ReadingSourceOperator(DriverContext driverContext, UsageStatus status) {
            super(List.of(new Page(driverContext.blockFactory().newConstantIntBlockWith(1, 1))).iterator());
            this.status = status;
        }

        @Override
        public Status status() {
            return status;
        }
    }

    /** A driver that failed, here aborted before it ran, with a source that read {@code status}. */
    private Driver failedDriver(UsageStatus status, Exception failure) {
        DriverContext driverContext = new DriverContext(nonBreakingBigArrays(), blockFactory(), null);
        Driver driver = TestDriverFactory.create(
            driverContext,
            new ReadingSourceOperator(driverContext, status),
            List.of(),
            new TestResultPageSinkOperator(page -> fail("sink should not receive output"))
        );
        PlainActionFuture<Void> aborted = new PlainActionFuture<>();
        driver.abort(failure, aborted);
        expectThrows(failure.getClass(), () -> aborted.actionGet(10, TimeUnit.SECONDS));
        return driver;
    }

    public void testCompletionListenerLearnsWhatTheFailedDriversDid() {
        Exception failure = new IllegalStateException("failed");
        Driver first = failedDriver(new UsageStatus(100, 10, 5), failure);
        Driver second = failedDriver(new UsageStatus(1_000, 20, 7), failure);
        long planningBytesRead = randomLongBetween(0, 50);
        AtomicReference<Exception> reportedFailure = new AtomicReference<>();
        AtomicReference<DriverCompletionInfo> reportedUsage = new AtomicReference<>();
        ComputeCompletionListener listener = new ComputeCompletionListener() {
            @Override
            public void onResponse(DriverCompletionInfo info) {
                fail("the compute failed");
            }

            @Override
            public void onFailure(Exception e, DriverCompletionInfo usage) {
                reportedFailure.set(e);
                reportedUsage.set(usage);
            }
        };

        ComputeService.failWithUsage(listener, failure, List.of(first, second), planningBytesRead, false);

        assertThat(reportedFailure.get(), sameInstance(failure));
        assertThat(reportedUsage.get().bytesRead(), equalTo(1_100 + planningBytesRead));
        assertThat(reportedUsage.get().readNanos(), equalTo(30L));
        assertThat(reportedUsage.get().readCpuNanos(), equalTo(12L));
    }

    public void testPlainListenerGetsAPlainFailure() {
        Exception failure = new IllegalStateException("failed");
        Driver driver = failedDriver(new UsageStatus(100, 10, 5), failure);
        AtomicReference<Exception> reportedFailure = new AtomicReference<>();
        ActionListener<DriverCompletionInfo> listener = ActionListener.wrap(info -> fail("the compute failed"), reportedFailure::set);

        ComputeService.failWithUsage(listener, failure, List.of(driver), 0, false);

        assertThat(reportedFailure.get(), sameInstance(failure));
    }
}
