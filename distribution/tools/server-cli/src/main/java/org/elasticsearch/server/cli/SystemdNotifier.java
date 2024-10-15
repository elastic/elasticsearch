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
import org.elasticsearch.nativeaccess.NativeAccess;
import org.elasticsearch.nativeaccess.Systemd;

import java.io.Closeable;
import java.io.IOException;
import java.util.Timer;
import java.util.TimerTask;

class SystemdNotifier implements ServerProcessListener, Closeable {

    private static final Logger logger = LogManager.getLogger(SystemdNotifier.class);
    private static final Systemd systemd = NativeAccess.instance().systemd();
    private final Timer timer;

    SystemdNotifier() {
        this.timer = new Timer("systemd-notifier", true);
        long timeBetweenExtend = TimeValue.timeValueSeconds(15).millis();
        TimerTask extendTask = new TimerTask() {
            @Override
            public void run() {
                systemd.notify_extend_timeout(30);
            }
        };
        timer.scheduleAtFixedRate(extendTask, timeBetweenExtend, timeBetweenExtend);
    }

    @Override
    public void onReady() {
        timer.cancel();
        systemd.notify_ready();
    }

    @Override
    public void close() throws IOException {
        timer.cancel();
        systemd.notify_stopping();
    }
}
