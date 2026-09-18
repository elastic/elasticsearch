/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.stateless.memory;

import org.elasticsearch.ExceptionsHelper;
import org.elasticsearch.discovery.MasterNotDiscoveredException;
import org.elasticsearch.logging.Level;
import org.elasticsearch.node.NodeClosedException;
import org.elasticsearch.transport.ConnectTransportException;

public class SizingDataTransmissionLogging {

    private SizingDataTransmissionLogging() {}

    public static Level getExceptionLogLevel(Exception exception) {
        return ExceptionsHelper.unwrap(
            exception,
            NodeClosedException.class,
            ConnectTransportException.class,
            MasterNotDiscoveredException.class
        ) == null ? Level.WARN : Level.DEBUG;
    }
}
