/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.cluster.coordination;

import org.elasticsearch.cluster.node.DiscoveryNodeUtils;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.test.AbstractWireSerializingTestCase;

import java.io.IOException;
import java.util.UUID;

public class JoinStatusWireSerializingTests extends AbstractWireSerializingTestCase<JoinStatus> {

    @Override
    protected Writeable.Reader<JoinStatus> instanceReader() {
        return JoinStatus::new;
    }

    @Override
    protected JoinStatus createTestInstance() {
        return new JoinStatus(
            DiscoveryNodeUtils.create(UUID.randomUUID().toString()),
            randomLongBetween(0, 1000),
            randomAlphaOfLengthBetween(0, 100),
            randomTimeValue()
        );
    }

    @Override
    protected JoinStatus mutateInstance(JoinStatus instance) throws IOException {
        // Since JoinStatus is a record, we don't need to check for equality
        return null;
    }
}
