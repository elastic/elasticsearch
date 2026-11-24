/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.cluster.routing;

import org.elasticsearch.test.ESTestCase;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.nullValue;
import static org.hamcrest.Matchers.sameInstance;

public class MoveReasonTests extends ESTestCase {

    public void testReasonWithoutDeciderIsShared() {
        for (final var type : MoveType.values()) {
            final var reason = MoveReason.of(type);
            assertThat(reason.type(), is(type));
            assertThat(reason.canRemainDecider(), nullValue());
            assertThat(MoveReason.of(type, null), sameInstance(reason));
        }
    }

    public void testReasonWithDecider() {
        final var type = randomFrom(MoveType.values());
        final var decider = randomIdentifier();
        assertThat(MoveReason.of(type, decider), equalTo(new MoveReason(type, decider)));
    }
}
