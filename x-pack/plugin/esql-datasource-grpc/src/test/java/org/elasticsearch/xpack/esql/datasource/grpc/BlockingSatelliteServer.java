/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasource.grpc;

import org.apache.arrow.flight.Action;
import org.apache.arrow.flight.CallStatus;
import org.apache.arrow.flight.FlightDescriptor;
import org.apache.arrow.flight.FlightEndpoint;
import org.apache.arrow.flight.FlightInfo;
import org.apache.arrow.flight.FlightServer;
import org.apache.arrow.flight.Location;
import org.apache.arrow.flight.NoOpFlightProducer;
import org.apache.arrow.flight.Result;
import org.apache.arrow.flight.SchemaResult;
import org.apache.arrow.flight.Ticket;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.IntVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.apache.arrow.vector.types.pojo.Schema;

import java.io.Closeable;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * A satellite that serves one table of three rows, {@code id integer}, and can be told to stall, so that tests of the connector can
 * cancel it, time it out or make it answer wrongly. It implements the capability action and records the fragments it is sent. It
 * does not evaluate them.
 */
public final class BlockingSatelliteServer implements Closeable {

    static final Schema SCHEMA = new Schema(List.of(new Field("id", FieldType.nullable(new ArrowType.Int(32, true)), null)));

    private static final String CAPABILITIES = "{\"specVersions\":[0],\"fragmentFormats\":[\"json\"],\"plan\":[\"scan\",\"limit\"],"
        + "\"functions\":[],\"types\":[\"integer\"],\"options\":[\"units\"]}";

    /** The schema of the stream and of the flight info it answers a fragment with. Tests change it to make the satellite wrong. */
    public volatile Schema answerSchema = SCHEMA;

    /** The body of the capability answer. Tests change it to make the satellite declare something else. */
    public volatile String capabilities = CAPABILITIES;

    /** The record count that {@code GetFlightInfo(path)} reports, -1 for unknown. */
    public volatile long recordCount = -1;
    /** When set, the capability action answers with this status instead of the declaration. */
    public volatile CallStatus failCapabilitiesWith;

    /** When set, the schema of {@code GetSchema} carries it as the declaration id (protocol section 5.2); null gives none. */
    public volatile String schemaDeclarationId;
    /** When set, {@code GetFlightInfo(command)} does not answer until {@link #release} is counted down. */
    public volatile boolean blockInFlightInfo;
    /** When set, {@code DoGet} does not send the first batch until released or cancelled by the client. */
    public volatile boolean blockInDoGet;
    /** When set, {@code DoGet} sends one batch and then stalls until released or cancelled by the client. */
    public volatile boolean stallAfterFirstBatch;
    /** When positive, {@code DoGet} sends a single batch of this many rows (a result far bigger than a small breaker allows). */
    public volatile int oneBatchOfRows;
    public final CountDownLatch inSecondBatchStall = new CountDownLatch(1);
    /** When set, {@code GetFlightInfo(command)} answers with this status, like a satellite that rejects the request. */
    public volatile CallStatus rejectFragmentWith;

    public final CountDownLatch inFlightInfo = new CountDownLatch(1);
    public final CountDownLatch inDoGet = new CountDownLatch(1);
    public final CountDownLatch release = new CountDownLatch(1);
    /** Counted down when the satellite noticed that the client cancelled the {@code DoGet} call. */
    public final CountDownLatch doGetCancelled = new CountDownLatch(1);
    /** How many times the capability action was called. */
    public final AtomicInteger capabilityCalls = new AtomicInteger();
    public final List<String> fragments = new CopyOnWriteArrayList<>();
    public final AtomicBoolean servedRows = new AtomicBoolean();

    private final BufferAllocator allocator = new RootAllocator();
    private final FlightServer server;
    private final Location location;

    public BlockingSatelliteServer() throws IOException {
        this.location = Location.forGrpcInsecure("127.0.0.1", 0);
        this.server = FlightServer.builder(allocator, location, new Producer()).build();
        this.server.start();
    }

    public int port() {
        return server.getPort();
    }

    private final class Producer extends NoOpFlightProducer {
        @Override
        public SchemaResult getSchema(CallContext context, FlightDescriptor descriptor) {
            String id = schemaDeclarationId;
            return new SchemaResult(
                id == null ? SCHEMA : new Schema(SCHEMA.getFields(), Map.of(SatelliteDeclarationCache.SCHEMA_METADATA_KEY, id))
            );
        }

        @Override
        public FlightInfo getFlightInfo(CallContext context, FlightDescriptor descriptor) {
            Location self = Location.forGrpcInsecure("127.0.0.1", server.getPort()); // spelled like the dataset resource: hosts are
                                                                                     // compared literally
            if (descriptor.isCommand() == false) {
                return new FlightInfo(
                    SCHEMA,
                    descriptor,
                    List.of(new FlightEndpoint(new Ticket("t".getBytes(StandardCharsets.UTF_8)), self)),
                    -1,
                    recordCount
                );
            }
            fragments.add(new String(descriptor.getCommand(), StandardCharsets.UTF_8));
            if (rejectFragmentWith != null) {
                throw rejectFragmentWith.toRuntimeException();
            }
            if (blockInFlightInfo) {
                inFlightInfo.countDown();
                await(release);
            }
            return new FlightInfo(
                answerSchema,
                descriptor,
                List.of(new FlightEndpoint(new Ticket("f".getBytes(StandardCharsets.UTF_8)))),
                -1,
                -1
            );
        }

        @Override
        public void doAction(CallContext context, Action action, StreamListener<Result> listener) {
            if ("esql.satellite.capabilities".equals(action.getType())) {
                capabilityCalls.incrementAndGet();
                if (failCapabilitiesWith != null) {
                    listener.onError(failCapabilitiesWith.toRuntimeException());
                    return;
                }
                listener.onNext(new Result(capabilities.getBytes(StandardCharsets.UTF_8)));
                listener.onCompleted();
            } else {
                super.doAction(context, action, listener);
            }
        }

        @Override
        public void getStream(CallContext context, Ticket ticket, ServerStreamListener listener) {
            if (blockInDoGet) {
                inDoGet.countDown();
                // like a satellite that sorts: it checks for a cancellation as it works instead of waiting for a callback
                long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(30);
                while (listener.isCancelled() == false && release.getCount() > 0 && System.nanoTime() < deadline) {
                    sleepQuietly(20);
                }
                if (listener.isCancelled()) {
                    doGetCancelled.countDown();
                    return;
                }
            }
            try (VectorSchemaRoot root = VectorSchemaRoot.create(answerSchema, allocator)) {
                listener.start(root);
                int rows = oneBatchOfRows > 0 ? oneBatchOfRows : 3;
                root.allocateNew();
                if (root.getVector(0) instanceof IntVector ids) {
                    for (int i = 0; i < rows; i++) {
                        ids.setSafe(i, i);
                    }
                }
                root.setRowCount(rows);
                listener.putNext();
                if (stallAfterFirstBatch) {
                    inSecondBatchStall.countDown();
                    long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(30);
                    while (listener.isCancelled() == false && release.getCount() > 0 && System.nanoTime() < deadline) {
                        sleepQuietly(20);
                    }
                    if (listener.isCancelled()) {
                        doGetCancelled.countDown();
                        return;
                    }
                }
                listener.completed();
                servedRows.set(true);
            }
        }
    }

    private static void sleepQuietly(long millis) {
        try {
            Thread.sleep(millis);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    private static void await(CountDownLatch latch) {
        try {
            latch.await(30, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    @Override
    public void close() throws IOException {
        release.countDown();
        try {
            server.close();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        } finally {
            allocator.close();
        }
    }
}
