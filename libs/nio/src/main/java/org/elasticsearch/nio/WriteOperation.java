/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
package org.elasticsearch.nio;

import java.util.function.BiConsumer;

/**
 * This is a basic write operation that can be queued with a channel. The only requirements of a write
 * operation is that is has a listener and a reference to its channel. The actual conversion of the write
 * operation implementation to bytes will be performed by the {@link NioChannelHandler}.
 */
public interface WriteOperation {

    BiConsumer<Void, Exception> getListener();

    SocketChannelContext getChannel();

    Object getObject();

}
