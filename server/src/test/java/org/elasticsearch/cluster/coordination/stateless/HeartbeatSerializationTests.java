/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.cluster.coordination.stateless;

import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.test.AbstractWireSerializingTestCase;

import java.io.IOException;

public class HeartbeatSerializationTests extends AbstractWireSerializingTestCase<Heartbeat> {
    @Override
    protected Writeable.Reader<Heartbeat> instanceReader() {
        return Heartbeat::new;
    }

    @Override
    protected Heartbeat createTestInstance() {
        return new Heartbeat(randomPositiveLong(), randomPositiveLong());
    }

    @Override
    protected Heartbeat mutateInstance(Heartbeat instance) throws IOException {
        if (randomBoolean()) {
            return new Heartbeat(randomPositiveLong(), instance.absoluteTimeInMillis());
        } else {
            return new Heartbeat(instance.term(), randomPositiveLong());
        }
    }

    private long randomPositiveLong() {
        return randomLongBetween(0, Long.MAX_VALUE);
    }
}
