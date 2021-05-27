/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.nio;

import java.io.Closeable;
import java.io.IOException;
import java.net.InetSocketAddress;

/**
 * An class for interfacing with java.nio. Implementations implement the underlying logic for opening
 * channels and registering them with the OS.
 */
public interface NioGroup extends Closeable {

    /**
     * Opens and binds a server channel to accept incoming connections.
     */
    <S extends NioServerSocketChannel> S bindServerChannel(InetSocketAddress address, ChannelFactory<S, ?> factory) throws IOException;

    /**
     * Opens a outgoing client channel.
     */
    <S extends NioSocketChannel> S openChannel(InetSocketAddress address, ChannelFactory<?, S> factory) throws IOException;

    @Override
    void close() throws IOException;
}
