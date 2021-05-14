/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
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
