/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.nativeaccess;

import org.elasticsearch.logging.LogManager;
import org.elasticsearch.logging.Logger;
import org.elasticsearch.nativeaccess.lib.PosixCLibrary;

import java.nio.charset.StandardCharsets;

public class Systemd {
    private static final Logger logger = LogManager.getLogger(Systemd.class);

    private final PosixCLibrary libc;
    private final String socketPath;
    private final CloseableByteBuffer buffer;

    Systemd(PosixCLibrary libc, String socketPath, CloseableByteBuffer buffer) {
        this.libc = libc;
        this.socketPath = socketPath;
        this.buffer = buffer;
    }

    /**
     * Notify systemd that the process is ready.
     *
     * @throws RuntimeException on failure to notify systemd
     */
    public void notify_ready() {
        notify("READY=1", false);
    }

    public void notify_extend_timeout(long seconds) {
        notify("EXTEND_TIMEOUT_USEC=" + (seconds * 1000000), true);
    }

    public void notify_stopping() {
        notify("STOPPING=1", true);
    }

    private void notify(String state, boolean warnOnError) {
        int sockfd = libc.socket(PosixCLibrary.AF_UNIX, PosixCLibrary.SOCK_DGRAM, 0);
        if (sockfd < 0) {
            throwOrLog("Could not open systemd socket: " + libc.strerror(libc.errno()), warnOnError);
            return;
        }
        RuntimeException error = null;
        try {
            var sockAddr = libc.newUnixSockAddr(socketPath);
            if (libc.connect(sockfd, sockAddr) != 0) {
                throwOrLog("Could not connect to systemd socket: " + libc.strerror(libc.errno()), warnOnError);
                return;
            }
            buffer.buffer().clear();
            byte[] bytes = state.getBytes(StandardCharsets.US_ASCII);
            buffer.buffer().put(0, bytes);
            buffer.buffer().limit(bytes.length);
            long bytesSent = libc.send(sockfd, buffer, 0);
            if (bytesSent == -1) {
                throwOrLog("Failed to send message (" + state + ") to systemd socket: " + libc.strerror(libc.errno()), warnOnError);
            } else if (bytesSent != bytes.length) {
                throwOrLog("Not all bytes of message (" + state + ") sent to systemd socket", warnOnError);
            } else {
                logger.info("Message (" + state + ") sent to systemd socket");
            }
        } catch (RuntimeException e) {
            error = e;
        } finally {
            if (libc.close(sockfd) != 0) {
                try {
                    throwOrLog("Could not close systemd socket: " + libc.strerror(libc.errno()), warnOnError);
                } catch (RuntimeException e) {
                    if (error != null) {
                        error.addSuppressed(e);
                        throw error;
                    } else {
                        throw e;
                    }
                }
            }
        }
    }

    private void throwOrLog(String message, boolean warnOnError) {
        if (warnOnError) {
            logger.warn(message);
        } else {
            throw new RuntimeException(message);
        }
    }
}
