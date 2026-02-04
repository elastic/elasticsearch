/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.cluster.coordination.stateless;

import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.test.AbstractWireSerializingTestCase;
import org.elasticsearch.test.ESTestCase;

import java.io.IOException;

public class HeartbeatSerializationTests extends AbstractWireSerializingTestCase<Heartbeat> {
    @Override
    protected Writeable.Reader<Heartbeat> instanceReader() {
        return Heartbeat::new;
    }

    @Override
    protected Heartbeat createTestInstance() {
        return new Heartbeat(randomNonNegativeLong(), randomNonNegativeLong());
    }

    @Override
    protected Heartbeat mutateInstance(Heartbeat instance) throws IOException {
        if (randomBoolean()) {
            return new Heartbeat(randomValueOtherThan(instance.term(), ESTestCase::randomNonNegativeLong), instance.absoluteTimeInMillis());
        } else {
            return new Heartbeat(instance.term(), randomValueOtherThan(instance.absoluteTimeInMillis(), ESTestCase::randomNonNegativeLong));
        }
    }
}
