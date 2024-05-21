/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.server.cli;

import org.elasticsearch.core.TimeValue;
import org.elasticsearch.logging.LogManager;
import org.elasticsearch.logging.Logger;

import java.io.Closeable;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.net.DatagramPacket;
import java.net.DatagramSocket;
import java.net.SocketException;
import java.net.UnixDomainSocketAddress;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.util.Locale;
import java.util.Timer;
import java.util.TimerTask;

class SystemdNotifier implements ServerProcessListener, Closeable {

    private static final Logger logger = LogManager.getLogger(SystemdNotifier.class);
    private final DatagramSocket socket;
    private final Timer timer;

    SystemdNotifier(Path notifySocketPath) {
        try {
            this.socket = new DatagramSocket(UnixDomainSocketAddress.of(notifySocketPath));
        } catch (SocketException e) {
            // TODO: should this be a UserException?
            throw new UncheckedIOException(e);
        }
        this.timer = new Timer("systemd-notifier", true);
        long timeBetweenExtend = TimeValue.timeValueSeconds(15).millis();
        long timeExtend = TimeValue.timeValueSeconds(30).micros();
        TimerTask extendTask = new TimerTask() {
            @Override
            public void run() {
                SystemdNotifier.this.notify("EXTEND_TIMEOUT_USEC=" + timeExtend, true);
            }
        };
        timer.scheduleAtFixedRate(extendTask, timeBetweenExtend, timeBetweenExtend);
    }

    private synchronized void notify(String state, boolean warnOnError) {
        logger.trace("systemd notify({})", 0, state);
        try {
            byte[] stateBytes = state.getBytes(StandardCharsets.UTF_8);
            socket.send(new DatagramPacket(stateBytes, stateBytes.length));
        } catch (IOException e) {
            String message = String.format(Locale.ROOT, "systemd notify(%s) returned error", state);
            if (warnOnError) {
                logger.warn(message, e);
            } else {
                throw new RuntimeException(message, e);
            }
        }

    }

    @Override
    public void onReady() {
        timer.cancel();
        notify("READY=1", false);
    }

    @Override
    public void close() {
        notify("STOPPING=1", true);
    }
}
