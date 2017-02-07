/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack;

import org.bouncycastle.operator.OperatorCreationException;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.xpack.support.clock.ClockMock;
import org.elasticsearch.xpack.watcher.test.TimeWarpedWatcher;

import javax.security.auth.DestroyFailedException;
import java.io.IOException;
import java.security.KeyStoreException;
import java.security.NoSuchAlgorithmException;
import java.security.UnrecoverableKeyException;
import java.security.cert.CertificateException;
import java.time.Clock;

public class TimeWarpedXPackPlugin extends XPackPlugin {
    private final ClockMock clock = new ClockMock();

    public TimeWarpedXPackPlugin(Settings settings) throws IOException, CertificateException, UnrecoverableKeyException,
            NoSuchAlgorithmException, KeyStoreException, DestroyFailedException, OperatorCreationException {
        super(settings);
        watcher = new TimeWarpedWatcher(settings);
    }

    @Override
    protected Clock getClock() {
        return clock;
    }
}
