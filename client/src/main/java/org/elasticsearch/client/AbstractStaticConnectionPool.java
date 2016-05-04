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
import java.util.function.Predicate;
import java.util.stream.Stream;

/**
 * Base static connection pool implementation that deals with mutable connections. Marks connections as dead/alive when needed.
 * Provides a stream of alive connections or dead ones that should be retried for each {@link #nextConnection()} call, which
 * allows to filter connections through a customizable {@link Predicate}, called connection selector.
 * In case the returned stream is empty a last resort dead connection should be retrieved by calling {@link #lastResortConnection()}
 * and resurrected so that a single request attempt can be performed.
 * The {@link #onSuccess(StatefulConnection)} method marks the connection provided as an argument alive.
 * The {@link #onFailure(StatefulConnection)} method marks the connection provided as an argument dead.
 * This base implementation doesn't define the list implementation that stores connections, so that concurrency can be
 * handled in the subclasses depending on the usecase (e.g. defining the list volatile when needed).
 */
public abstract class AbstractStaticConnectionPool implements ConnectionPool<StatefulConnection> {

    private static final Log logger = LogFactory.getLog(AbstractStaticConnectionPool.class);

    private final AtomicInteger lastConnectionIndex = new AtomicInteger(0);

    protected abstract List<StatefulConnection> getConnections();

    @Override
    public final Stream<StatefulConnection> nextConnection() {
        List<StatefulConnection> connections = getConnections();
        if (connections.isEmpty()) {
            throw new IllegalStateException("no connections available in the connection pool");
        }

        List<StatefulConnection> sortedConnections = new ArrayList<>(connections);
        //TODO is it possible to make this O(1)? (rotate is O(n))
        Collections.rotate(sortedConnections, sortedConnections.size() - lastConnectionIndex.getAndIncrement());
        return sortedConnections.stream().filter(connection -> connection.isAlive() || connection.shouldBeRetried());
    }

    protected List<StatefulConnection> createConnections(HttpHost... hosts) {
        List<StatefulConnection> connections = new ArrayList<>();
        for (HttpHost host : hosts) {
            Objects.requireNonNull(host, "host cannot be null");
            connections.add(new StatefulConnection(host));
        }
        return Collections.unmodifiableList(connections);
    }

    @Override
    public StatefulConnection lastResortConnection() {
        StatefulConnection statefulConnection = getConnections().stream()
                .sorted((o1, o2) -> Long.compare(o1.getDeadUntil(), o2.getDeadUntil())).findFirst().get();
        statefulConnection.markResurrected();
        return statefulConnection;
    }

    @Override
    public void onSuccess(StatefulConnection connection) {
        connection.markAlive();
        logger.trace("marked connection alive for " + connection.getHost());
    }

    @Override
    public void onFailure(StatefulConnection connection) throws IOException {
        connection.markDead();
        logger.debug("marked connection dead for " + connection.getHost());
    }
}
