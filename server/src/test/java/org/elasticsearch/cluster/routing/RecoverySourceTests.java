/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.cluster.routing;

import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.elasticsearch.test.ESTestCase;

import java.io.IOException;

import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.hamcrest.Matchers.lessThanOrEqualTo;

public class RecoverySourceTests extends ESTestCase {

    public void testSerialization() throws IOException {
        RecoverySource recoverySource = TestShardRouting.buildRecoverySource();
        BytesStreamOutput out = new BytesStreamOutput();
        recoverySource.writeTo(out);
        RecoverySource serializedRecoverySource = RecoverySource.readFrom(out.bytes().streamInput());
        assertEquals(recoverySource.getType(), serializedRecoverySource.getType());
        assertEquals(recoverySource, serializedRecoverySource);
    }

    public void testRecoverySourceTypeOrder() {
        assertEquals(RecoverySource.Type.EMPTY_STORE.ordinal(), 0);
        assertEquals(RecoverySource.Type.EXISTING_STORE.ordinal(), 1);
        assertEquals(RecoverySource.Type.PEER.ordinal(), 2);
        assertEquals(RecoverySource.Type.SNAPSHOT.ordinal(), 3);
        assertEquals(RecoverySource.Type.LOCAL_SHARDS.ordinal(), 4);
        assertEquals(RecoverySource.Type.RESHARD_SPLIT.ordinal(), 5);
        // check exhaustiveness
        for (RecoverySource.Type type : RecoverySource.Type.values()) {
            assertThat(type.ordinal(), greaterThanOrEqualTo(0));
            assertThat(type.ordinal(), lessThanOrEqualTo(5));
        }
    }
}
