/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasource.grpc;

import org.apache.arrow.flight.CallOption;
import org.apache.arrow.flight.CallOptions;
import org.apache.arrow.flight.FlightClient;
import org.apache.arrow.flight.FlightDescriptor;
import org.apache.arrow.flight.FlightInfo;
import org.apache.arrow.flight.FlightRuntimeException;
import org.apache.arrow.flight.FlightStatusCode;
import org.apache.arrow.flight.FlightStream;
import org.apache.arrow.flight.Ticket;
import org.elasticsearch.ExceptionsHelper;
import org.elasticsearch.common.breaker.CircuitBreakingException;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentFactory;
import org.elasticsearch.xpack.esql.datasources.spi.Connector;
import org.elasticsearch.xpack.esql.datasources.spi.ExternalSplit;
import org.elasticsearch.xpack.esql.datasources.spi.QueryRequest;
import org.elasticsearch.xpack.esql.datasources.spi.ResultCursor;
import org.elasticsearch.xpack.esql.datasources.spi.SatelliteFragment;
import org.elasticsearch.xpack.esql.datasources.spi.Split;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.net.URI;
import java.util.Base64;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.concurrent.TimeUnit;

/**
 * Connector for Apache Arrow Flight endpoints.
 * <p>
 * Every {@code execute} opens its own {@link FlightClient} on the allocator of the calling driver and closes it with the cursor
 * (or at once if opening the stream fails). Clients cannot be shared between drivers: a client keeps the allocator it was built
 * with, which belongs to the first driver and is closed with it, so the other drivers would read into a closed allocator.
 * A {@link FlightSplit} that names a location is read from that location, otherwise from the configured endpoint.
 */
class FlightConnector implements Connector {

    private final URI endpointURI;
    private final FlightClients.Transport transport;
    private final FlightGuard hosts;
    private final CallOption callTimeout;
    private final SatelliteDeclarationCache declarations;
    private volatile boolean closed = false;

    FlightConnector(String endpoint) {
        this(endpoint, GrpcDataSourcePlugin.DEFAULT_CALL_TIMEOUT);
    }

    FlightConnector(String endpoint, TimeValue callTimeout) {
        this(endpoint, callTimeout, new SatelliteDeclarationCache(TimeValue.ZERO));
    }

    FlightConnector(String endpoint, TimeValue callTimeout, SatelliteDeclarationCache declarations) {
        this(endpoint, FlightClients.transport(endpoint, null), callTimeout, declarations);
    }

    FlightConnector(String endpoint, FlightClients.Transport transport, TimeValue callTimeout, SatelliteDeclarationCache declarations) {
        this(endpoint, transport, callTimeout, declarations, FlightGuard.DEFAULT);
    }

    FlightConnector(
        String endpoint,
        FlightClients.Transport transport,
        TimeValue callTimeout,
        SatelliteDeclarationCache declarations,
        FlightGuard hosts
    ) {
        this.hosts = hosts;
        this.endpointURI = URI.create(endpoint);
        this.transport = transport;
        this.callTimeout = CallOptions.timeout(callTimeout.millis(), TimeUnit.MILLISECONDS);
        this.declarations = declarations;
    }

    @Override
    public ResultCursor execute(QueryRequest request, Split split) {
        return doExecute(request, null);
    }

    @Override
    public ResultCursor execute(QueryRequest request, ExternalSplit split) {
        return doExecute(request, split instanceof FlightSplit fs ? fs : null);
    }

    /**
     * Returns at once: no call is made until the first {@code hasNext}, so the cursor already exists, and can be cancelled, while
     * the satellite plans, sorts or reads. A satellite whose fragment is a {@code topn} reads all its input before the first batch.
     */
    private ResultCursor doExecute(QueryRequest request, FlightSplit flightSplit) {
        if (closed) {
            throw new IllegalStateException("Connector is closed");
        }
        if (request.fragment() == null && request.config() != null) {
            // source options only travel inside a fragment (protocol section 11): the planner always sends one for such a dataset,
            // so reaching here means a plan shape it did not see, and reading without the options would change the results
            Map<String, Object> options = FlightConnectorFactory.optionsOf(request.config());
            // the table parameters reach the satellite in headers, fragment or not
            if (request.config().get(SatelliteFragment.TABLE_PARAMETERS_KEY) instanceof List<?> parameters) {
                options.keySet().removeAll(parameters);
            }
            if (options.isEmpty() == false) {
                throw new IllegalStateException(
                    "the dataset sets the source options " + options.keySet() + " but this read sends no fragment, so they would be ignored"
                );
            }
        }
        return new LazyCursor(request, flightSplit);
    }

    /** The ticket of a plain read: the split's own, or the first endpoint of the table when there is no split. */
    private Ticket readTicket(FlightClient client, QueryRequest request, FlightSplit flightSplit) {
        if (flightSplit != null) {
            return new Ticket(flightSplit.ticketBytes());
        }
        FlightInfo info = client.getInfo(FlightDescriptor.path(request.target()), callTimeout);
        if (info.getEndpoints().isEmpty()) {
            throw new IllegalStateException("Flight server returned no endpoints for target: " + request.target());
        }
        return info.getEndpoints().get(0).getTicket();
    }

    /**
     * Flight tickets are opaque and issued by the server, so the fragment travels in a command descriptor, together with the
     * partition reference (the split's own ticket); the satellite answers with the ticket of the real result stream (protocol
     * section 5.3). The cost is one extra round trip per split. Without a split the satellite reads the whole table.
     */
    private Ticket fragmentTicket(FlightClient client, QueryRequest request, FlightSplit flightSplit) {
        Map<String, Object> fragment = new LinkedHashMap<>(request.fragment());
        if (flightSplit != null) {
            fragment.put("partitions", List.of(Base64.getEncoder().encodeToString(flightSplit.ticketBytes())));
        }
        FlightInfo info = client.getInfo(FlightDescriptor.command(toJsonBytes(fragment)), callTimeout);
        if (info.getEndpoints().size() != 1) {
            throw new IllegalStateException(
                "satellite answered a fragment with [" + info.getEndpoints().size() + "] endpoints, expected one"
            );
        }
        SatelliteSchemaCheck.verify(info.getSchema(), request.attributes(), "flight info schema");
        return info.getEndpoints().get(0).getTicket();
    }

    /**
     * A satellite that answers {@code INVALID_ARGUMENT} or {@code NOT_FOUND} is saying the request is wrong (a bad option value, a
     * table that is gone): that is the caller's to fix, so it surfaces as a client error carrying the satellite's message and not as
     * an unexpected server failure. Anything else is not a rejection and is rethrown as it is (null here).
     */
    /**
     * Arrow allocates the memory of a batch on gRPC's network thread, where the circuit breaker may trip; Flight reports that as a
     * cancelled call whose cause is the breaker's exception. The breaker's own exception is what the caller must see (a 429), not an
     * unexpected failure of the remote server.
     */
    static RuntimeException unwrapBreaker(RuntimeException e) {
        return ExceptionsHelper.unwrap(e, CircuitBreakingException.class) instanceof CircuitBreakingException breaker ? breaker : e;
    }

    private static IllegalArgumentException rejection(Throwable t) {
        IllegalArgumentException authentication = FlightClients.authenticationFailure(t);
        if (authentication != null) {
            return authentication;
        }
        if (t instanceof FlightRuntimeException e) {
            FlightStatusCode code = e.status().code();
            if (code == FlightStatusCode.INVALID_ARGUMENT || code == FlightStatusCode.NOT_FOUND) {
                String description = e.status().description();
                return new IllegalArgumentException(
                    "the satellite rejected the request ["
                        + code
                        + "]: "
                        + FlightClients.satelliteText(description == null ? e.getMessage() : description),
                    e
                );
            }
        }
        return null;
    }

    private static byte[] toJsonBytes(Map<String, Object> fragment) {
        try (XContentBuilder builder = XContentFactory.jsonBuilder()) {
            return BytesReference.toBytes(BytesReference.bytes(builder.map(fragment)));
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    private FlightClient openClient(FlightSplit split, QueryRequest request) {
        URI uri = split != null && split.location() != null ? URI.create(split.location()) : endpointURI;
        String host = uri.getHost();
        if (host == null || host.isBlank()) {
            throw new IllegalArgumentException("Invalid Flight endpoint location: missing host");
        }
        // the transport is the endpoint's: a location a satellite names can change the host, never the security of the connection
        if ((transport.token() != null || transport.credentials() != null)
            && (host.equalsIgnoreCase(endpointURI.getHost()) && getPort(uri) == getPort(endpointURI)) == false) {
            // a location a satellite names must not receive the credentials of the endpoint the dataset names
            throw new IllegalArgumentException("the satellite named another host for a read that needs credentials; it is not sent them");
        }
        return FlightClients.build(request.blockFactory().arrowAllocator(), host, getPort(uri), transport, hosts);
    }

    private static void closeQuietly(AutoCloseable closeable) {
        if (closeable != null) {
            try {
                closeable.close();
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            } catch (Exception ignored) {
                // the original failure is the one to report
            }
        }
    }

    @Override
    public void close() {
        closed = true;
    }

    private static int getPort(URI uri) {
        return uri.getPort() > 0 ? uri.getPort() : FlightConnectorFactory.DEFAULT_FLIGHT_PORT;
    }

    @Override
    public String toString() {
        return "FlightConnector";
    }

    /**
     * The read of one split. It opens its own client on the allocator of the calling driver (a client keeps the allocator it was
     * built with, so one cannot be shared between drivers), makes the calls on the first {@code hasNext} and closes the client with
     * the cursor, or at once when a call fails. {@link #cancel} may come from any thread at any point: it cancels the stream when
     * there is one, which wakes a read blocked on the first batch, and otherwise is applied as soon as the stream exists. A
     * cancelled cursor has no more rows.
     */
    private final class LazyCursor implements ResultCursor {
        private final QueryRequest request;
        private final FlightSplit split;
        private volatile boolean cancelled;
        private volatile boolean closed;
        private volatile FlightStream stream;
        private FlightClient client;
        private volatile ResultCursor delegate;

        LazyCursor(QueryRequest request, FlightSplit split) {
            this.request = request;
            this.split = split;
        }

        private boolean started() {
            if (delegate != null) {
                return true;
            }
            if (cancelled || closed) {
                return false;
            }
            try {
                client = openClient(split, request);
                Ticket ticket = request.fragment() != null ? fragmentTicket(client, request, split) : readTicket(client, request, split);
                if (cancelled) {
                    return false;
                }
                stream = client.getStream(ticket);
                if (cancelled) {
                    stream.cancel("query cancelled", null);
                    closeQuietly(stream); // no cursor owns the stream yet, so close() would not find it
                    stream = null;
                    return false;
                }
                delegate = new FlightResultCursor(
                    stream,
                    request.attributes(),
                    request.blockFactory(),
                    request.fragment() != null,
                    hosts.limits().streamIdleTimeout(),
                    hosts.limits().maxColumns()
                );
                return true;
            } catch (Throwable t) {
                closeQuietly(stream);
                closeQuietly(client);
                client = null;
                stream = null;
                if (cancelled) {
                    return false; // the call failed because it was cancelled: that is the end of the rows, not a failure
                }
                IllegalArgumentException rejected = rejection(t);
                if (rejected != null) {
                    if (request.fragment() != null) {
                        // the fragment was built from a declaration this node may have kept: read it again for the next query
                        declarations.invalidate(endpointURI.toString() + transport.cacheScope(), request.target());
                    }
                    throw rejected;
                }
                if (t instanceof RuntimeException e) {
                    throw unwrapBreaker(e);
                }
                throw t;
            }
        }

        @Override
        public boolean hasNext() {
            return started() && delegate.hasNext();
        }

        @Override
        public Page next() {
            if (started() == false) {
                throw new NoSuchElementException();
            }
            return delegate.next();
        }

        @Override
        public void cancel() {
            cancelled = true;
            ResultCursor reading = delegate;
            if (reading != null) {
                reading.cancel(); // also tells the cursor that a failed wait for the next batch is the end, not an error
                return;
            }
            FlightStream active = stream;
            if (active != null) {
                active.cancel("query cancelled", null);
            }
        }

        @Override
        public void close() throws IOException {
            closed = true;
            try {
                if (delegate != null) {
                    delegate.close();
                } else {
                    closeQuietly(stream);
                }
            } finally {
                closeQuietly(client);
            }
        }
    }
}
