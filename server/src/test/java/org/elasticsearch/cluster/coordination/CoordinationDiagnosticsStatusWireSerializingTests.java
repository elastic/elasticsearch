/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.cluster.coordination;

import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.test.AbstractWireSerializingTestCase;

import java.io.IOException;

import static org.elasticsearch.cluster.coordination.CoordinationDiagnosticsService.CoordinationDiagnosticsStatus;

/**
 * Wire serialization tests for {@link CoordinationDiagnosticsStatus}.
 */
public class CoordinationDiagnosticsStatusWireSerializingTests
    extends AbstractWireSerializingTestCase<CoordinationDiagnosticsStatus> {

    @Override
    protected Writeable.Reader<CoordinationDiagnosticsStatus> instanceReader() {
        return CoordinationDiagnosticsStatus::fromStreamInput;
    }

    @Override
    protected CoordinationDiagnosticsStatus createTestInstance() {
        return randomFrom(CoordinationDiagnosticsStatus.values());
    }

    @Override
    protected CoordinationDiagnosticsStatus mutateInstance(CoordinationDiagnosticsStatus instance) throws IOException {
        return randomValueOtherThan(instance, () -> randomFrom(CoordinationDiagnosticsStatus.values()));
    }

    /**
     * Enum deserialization returns the same singleton constant instance, so the round-trip copy
     * is the same reference as the original. Override so the base class uses assertSame instead
     * of assertNotSame.
     */
    @Override
    protected boolean shouldBeSame(CoordinationDiagnosticsStatus newInstance) {
        return true;
    }
}
