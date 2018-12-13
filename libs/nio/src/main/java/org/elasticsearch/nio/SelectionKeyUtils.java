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

package org.elasticsearch.nio;

import java.nio.channels.CancelledKeyException;
import java.nio.channels.SelectionKey;

public final class SelectionKeyUtils {

    private SelectionKeyUtils() {}

    /**
     * Adds an interest in writes for this selection key while maintaining other interests.
     *
     * @param selectionKey the selection key
     * @throws CancelledKeyException if the key was already cancelled
     */
    public static void setWriteInterested(SelectionKey selectionKey) throws CancelledKeyException {
        selectionKey.interestOps(selectionKey.interestOps() | SelectionKey.OP_WRITE);
    }

    /**
     * Removes an interest in writes for this selection key while maintaining other interests.
     *
     * @param selectionKey the selection key
     * @throws CancelledKeyException if the key was already cancelled
     */
    public static void removeWriteInterested(SelectionKey selectionKey) throws CancelledKeyException {
        selectionKey.interestOps(selectionKey.interestOps() & ~SelectionKey.OP_WRITE);
    }

    /**
     * Removes an interest in connects and reads for this selection key while maintaining other interests.
     *
     * @param selectionKey the selection key
     * @throws CancelledKeyException if the key was already cancelled
     */
    public static void setConnectAndReadInterested(SelectionKey selectionKey) throws CancelledKeyException {
        selectionKey.interestOps(selectionKey.interestOps() | SelectionKey.OP_CONNECT | SelectionKey.OP_READ);
    }

    /**
     * Removes an interest in connects, reads, and writes for this selection key while maintaining other
     * interests.
     *
     * @param selectionKey the selection key
     * @throws CancelledKeyException if the key was already cancelled
     */
    public static void setConnectReadAndWriteInterested(SelectionKey selectionKey) throws CancelledKeyException {
        selectionKey.interestOps(selectionKey.interestOps() | SelectionKey.OP_CONNECT | SelectionKey.OP_READ | SelectionKey.OP_WRITE);
    }

    /**
     * Removes an interest in connects for this selection key while maintaining other interests.
     *
     * @param selectionKey the selection key
     * @throws CancelledKeyException if the key was already cancelled
     */
    public static void removeConnectInterested(SelectionKey selectionKey) throws CancelledKeyException {
        selectionKey.interestOps(selectionKey.interestOps() & ~SelectionKey.OP_CONNECT);
    }

    /**
     * Adds an interest in accepts for this selection key while maintaining other interests.
     *
     * @param selectionKey the selection key
     * @throws CancelledKeyException if the key was already cancelled
     */
    public static void setAcceptInterested(SelectionKey selectionKey) throws CancelledKeyException {
        selectionKey.interestOps(selectionKey.interestOps() | SelectionKey.OP_ACCEPT);
    }


    /**
     * Checks for an interest in writes for this selection key.
     *
     * @param selectionKey the selection key
     * @return a boolean indicating if we are currently interested in writes for this channel
     * @throws CancelledKeyException if the key was already cancelled
     */
    public static boolean isWriteInterested(SelectionKey selectionKey) throws CancelledKeyException {
        return (selectionKey.interestOps() & SelectionKey.OP_WRITE) != 0;
    }
}
