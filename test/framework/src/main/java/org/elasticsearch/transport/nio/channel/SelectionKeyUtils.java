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

package org.elasticsearch.transport.nio.channel;

import java.nio.channels.CancelledKeyException;
import java.nio.channels.SelectionKey;

public final class SelectionKeyUtils {

    private SelectionKeyUtils() {}

    public static void setWriteInterested(NioChannel channel) throws CancelledKeyException {
        SelectionKey selectionKey = channel.getSelectionKey();
        selectionKey.interestOps(selectionKey.interestOps() | SelectionKey.OP_WRITE);
    }

    public static void removeWriteInterested(NioChannel channel) throws CancelledKeyException {
        SelectionKey selectionKey = channel.getSelectionKey();
        selectionKey.interestOps(selectionKey.interestOps() & ~SelectionKey.OP_WRITE);
    }

    public static void setConnectAndReadInterested(NioChannel channel) throws CancelledKeyException {
        SelectionKey selectionKey = channel.getSelectionKey();
        selectionKey.interestOps(selectionKey.interestOps() | SelectionKey.OP_CONNECT | SelectionKey.OP_READ);
    }

    public static void removeConnectInterested(NioChannel channel) throws CancelledKeyException {
        SelectionKey selectionKey = channel.getSelectionKey();
        selectionKey.interestOps(selectionKey.interestOps() & ~SelectionKey.OP_CONNECT);
    }

    public static void setAcceptInterested(NioServerSocketChannel channel) {
        SelectionKey selectionKey = channel.getSelectionKey();
        selectionKey.interestOps(selectionKey.interestOps() | SelectionKey.OP_ACCEPT);
    }
}
