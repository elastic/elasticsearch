/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.elasticsearch.client;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.http.HttpHost;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Stream;

/**
 * Base static connection pool implementation that marks connections as dead/alive when needed.
 * Provides a stream of alive connections or dead ones that should be retried for each {@link #nextConnection()} call.
 * In case the returned stream is empty a last resort dead connection should be retrieved by calling {@link #lastResortConnection()}
 * and resurrected so that a last resort request attempt can be performed.
 * The {@link #onSuccess(Connection)} method marks the connection provided as an argument alive.
 * The {@link #onFailure(Connection)} method marks the connection provided as an argument dead.
 * This base implementation doesn't define the list implementation that stores connections, so that concurrency can be
 * handled in subclasses depending on the usecase (e.g. defining the list volatile or final when needed).
 */
public abstract class AbstractStaticConnectionPool implements ConnectionPool {

    private static final Log logger = LogFactory.getLog(AbstractStaticConnectionPool.class);

    private final AtomicInteger lastConnectionIndex = new AtomicInteger(0);

    /**
     * Allows to retrieve the concrete list of connections. Not defined directly as a member
     * of this class as subclasses may need to handle concurrency if the list can change, for
     * instance defining the field as volatile. On the other hand static implementations
     * can just make the list final instead.
     */
    protected abstract List<Connection> getConnections();

    @Override
    public final Stream<Connection> nextConnection() {
        List<Connection> connections = getConnections();
        if (connections.isEmpty()) {
            throw new IllegalStateException("no connections available in the connection pool");
        }

        List<Connection> sortedConnections = new ArrayList<>(connections);
        //TODO is it possible to make this O(1)? (rotate is O(n))
        Collections.rotate(sortedConnections, sortedConnections.size() - lastConnectionIndex.getAndIncrement());
        return sortedConnections.stream().filter(connection -> connection.isAlive() || connection.shouldBeRetried());
    }

    /**
     * Helper method to be used by subclasses when needing to create a new list
     * of connections given their corresponding hosts
     */
    protected List<Connection> createConnections(HttpHost... hosts) {
        List<Connection> connections = new ArrayList<>();
        for (HttpHost host : hosts) {
            Objects.requireNonNull(host, "host cannot be null");
            connections.add(new Connection(host));
        }
        return Collections.unmodifiableList(connections);
    }

    @Override
    public Connection lastResortConnection() {
        Connection Connection = getConnections().stream()
                .sorted((o1, o2) -> Long.compare(o1.getDeadUntil(), o2.getDeadUntil())).findFirst().get();
        Connection.markResurrected();
        return Connection;
    }

    @Override
    public void onSuccess(Connection connection) {
        connection.markAlive();
        logger.trace("marked connection alive for " + connection.getHost());
    }

    @Override
    public void onFailure(Connection connection) throws IOException {
        connection.markDead();
        logger.debug("marked connection dead for " + connection.getHost());
    }
}
