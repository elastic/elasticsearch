/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.operator.exchange;

import org.elasticsearch.TransportVersion;
import org.elasticsearch.TransportVersions;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.compute.operator.DriverContext;
import org.elasticsearch.compute.operator.IsBlockedResult;
import org.elasticsearch.compute.operator.Operator;
import org.elasticsearch.compute.operator.SinkOperator;
import org.elasticsearch.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.Objects;
import java.util.function.Supplier;

/**
 * Sink operator implementation that pushes data to an {@link ExchangeSink}
 */
public class ExchangeSinkOperator extends SinkOperator {

    private final ExchangeSink sink;
    private int pagesReceived;
    private long rowsReceived;

    public record ExchangeSinkOperatorFactory(Supplier<ExchangeSink> exchangeSinks) implements SinkOperatorFactory {

        @Override
        public SinkOperator get(DriverContext driverContext) {
            return new ExchangeSinkOperator(exchangeSinks.get());
        }

        @Override
        public String describe() {
            return "ExchangeSinkOperator";
        }
    }

    public ExchangeSinkOperator(ExchangeSink sink) {
        this.sink = sink;
    }

    @Override
    public boolean isFinished() {
        return sink.isFinished();
    }

    public void addCompletionListener(ActionListener<Void> listener) {
        sink.addCompletionListener(listener);
    }

    @Override
    public void finish() {
        sink.finish();
    }

    @Override
    public IsBlockedResult isBlocked() {
        return sink.waitForWriting();
    }

    @Override
    public boolean needsInput() {
        return isFinished() == false && isBlocked().listener().isDone();
    }

    @Override
    protected void doAddInput(Page page) {
        pagesReceived++;
        rowsReceived += page.getPositionCount();
        sink.addPage(page);
    }

    @Override
    public void close() {
        finish();
    }

    @Override
    public String toString() {
        return "ExchangeSinkOperator";
    }

    @Override
    public Status status() {
        return new Status(pagesReceived, rowsReceived);
    }

    public static class Status implements Operator.Status {
        public static final NamedWriteableRegistry.Entry ENTRY = new NamedWriteableRegistry.Entry(
            Operator.Status.class,
            "exchange_sink",
            Status::new
        );

        private final int pagesReceived;
        private final long rowsReceived;

        Status(int pagesReceived, long rowsReceived) {
            this.pagesReceived = pagesReceived;
            this.rowsReceived = rowsReceived;
        }

        Status(StreamInput in) throws IOException {
            pagesReceived = in.readVInt();

            if (in.getTransportVersion().onOrAfter(TransportVersions.ESQL_PROFILE_ROWS_PROCESSED)) {
                rowsReceived = in.readVLong();
            } else {
                rowsReceived = 0;
            }
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            out.writeVInt(pagesReceived);

            if (out.getTransportVersion().onOrAfter(TransportVersions.ESQL_PROFILE_ROWS_PROCESSED)) {
                out.writeVLong(rowsReceived);
            }
        }

        @Override
        public String getWriteableName() {
            return ENTRY.name;
        }

        public int pagesReceived() {
            return pagesReceived;
        }

        public long rowsReceived() {
            return rowsReceived;
        }

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            builder.startObject();
            builder.field("pages_received", pagesReceived);
            builder.field("rows_received", rowsReceived);
            return builder.endObject();
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            Status status = (Status) o;
            return pagesReceived == status.pagesReceived && rowsReceived == status.rowsReceived;
        }

        @Override
        public int hashCode() {
            return Objects.hash(pagesReceived, rowsReceived);
        }

        @Override
        public String toString() {
            return Strings.toString(this);
        }

        @Override
        public TransportVersion getMinimalSupportedVersion() {
            return TransportVersions.V_8_11_X;
        }
    }
}
