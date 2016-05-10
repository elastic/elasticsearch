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

import java.io.Closeable;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Pool of connections to the different hosts that belong to an elasticsearch cluster.
 * It keeps track of the different hosts to communicate with and allows to retrieve an iterator of connections to be used
 * for each request. Marks connections as dead/alive when needed.
 * Provides an iterator of connections to be used at each {@link #nextConnection()} call.
 * The {@link #onSuccess(Connection)} method marks the connection provided as an argument alive.
 * The {@link #onFailure(Connection)} method marks the connection provided as an argument dead.
 * This base implementation doesn't define the list implementation that stores connections, so that concurrency can be
 * handled in subclasses depending on the usecase (e.g. defining the list volatile or final when needed).
 */
public abstract class ConnectionPool implements Closeable {

    private static final Log logger = LogFactory.getLog(ConnectionPool.class);

    private final AtomicInteger lastConnectionIndex = new AtomicInteger(0);

    /**
     * Allows to retrieve the concrete list of connections. Not defined directly as a member
     * of this class as subclasses may need to handle concurrency if the list can change, for
     * instance defining the field as volatile. On the other hand static implementations
     * can just make the list final instead.
     */
    protected abstract List<Connection> getConnections();

    /**
     * Returns an iterator of connections that should be used for a request call.
     * Ideally, the first connection is retrieved from the iterator and used successfully for the request.
     * Otherwise, after each failure the next connection should be retrieved from the iterator so that the request can be retried.
     * The maximum total of attempts is equal to the number of connections that are available in the iterator.
     * The iterator returned will never be empty, rather an {@link IllegalStateException} will be thrown in that case.
     * In case there are no alive connections available, or dead ones that should be retried, one dead connection
     * gets resurrected and returned.
     */
    public final Iterator<Connection> nextConnection() {
        List<Connection> connections = getConnections();
        if (connections.isEmpty()) {
            throw new IllegalStateException("no connections available in the connection pool");
        }

        List<Connection> rotatedConnections = new ArrayList<>(connections);
        //TODO is it possible to make this O(1)? (rotate is O(n))
        Collections.rotate(rotatedConnections, rotatedConnections.size() - lastConnectionIndex.getAndIncrement());
        Iterator<Connection> connectionIterator = rotatedConnections.iterator();
        while (connectionIterator.hasNext()) {
            Connection connection = connectionIterator.next();
            if (connection.isAlive() == false && connection.shouldBeRetried() == false) {
                connectionIterator.remove();
            }
        }
        if (rotatedConnections.isEmpty()) {
            List<Connection> sortedConnections = new ArrayList<>(connections);
            Collections.sort(sortedConnections, new Comparator<Connection>() {
                @Override
                public int compare(Connection o1, Connection o2) {
                    return Long.compare(o1.getDeadUntil(), o2.getDeadUntil());
                }
            });
            Connection connection = sortedConnections.get(0);
            connection.markResurrected();
            logger.trace("marked connection resurrected for " + connection.getHost());
            return Collections.singleton(connection).iterator();
        }
        return rotatedConnections.iterator();
    }

    /**
     * Helper method to be used by subclasses when needing to create a new list
     * of connections given their corresponding hosts
     */
    protected final List<Connection> createConnections(HttpHost... hosts) {
        List<Connection> connections = new ArrayList<>();
        for (HttpHost host : hosts) {
            Objects.requireNonNull(host, "host cannot be null");
            connections.add(new Connection(host));
        }
        return Collections.unmodifiableList(connections);
    }

    /**
     * Called after each successful request call.
     * Receives as an argument the connection that was used for the successful request.
     */
    public void onSuccess(Connection connection) {
        connection.markAlive();
        logger.trace("marked connection alive for " + connection.getHost());
    }

    /**
     * Called after each failed attempt.
     * Receives as an argument the connection that was used for the failed attempt.
     */
    public void onFailure(Connection connection) throws IOException {
        connection.markDead();
        logger.debug("marked connection dead for " + connection.getHost());
    }
}
