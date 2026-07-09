/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.indices.recovery;

import org.elasticsearch.common.UUIDs;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.test.AbstractWireSerializingTestCase;

import java.io.IOException;
import java.util.HashSet;
import java.util.Set;

public class CancelRecoveriesActionResponseTests extends AbstractWireSerializingTestCase<CancelRecoveriesAction.Response> {

    @Override
    protected Writeable.Reader<CancelRecoveriesAction.Response> instanceReader() {
        return CancelRecoveriesAction.Response::new;
    }

    @Override
    protected CancelRecoveriesAction.Response createTestInstance() {
        return new CancelRecoveriesAction.Response(randomCancelledInQueue());
    }

    @Override
    protected CancelRecoveriesAction.Response mutateInstance(CancelRecoveriesAction.Response instance) throws IOException {
        return new CancelRecoveriesAction.Response(randomValueOtherThan(instance.cancelledInQueue(), this::randomCancelledInQueue));
    }

    private Set<String> randomCancelledInQueue() {
        return new HashSet<>(randomList(0, 5, UUIDs::randomBase64UUID));
    }
}
