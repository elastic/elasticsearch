/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.stateless.recovery;

import org.elasticsearch.TransportVersion;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.test.AbstractWireSerializingTestCase;
import org.elasticsearch.test.TransportVersionUtils;

import java.io.IOException;

import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.nullValue;

public class StartRelocationResponseSerializationTests extends AbstractWireSerializingTestCase<StartRelocationResponse> {

    private static final TransportVersion METRICS_IN_RESPONSE = TransportVersion.fromName("relocation_source_metrics_in_response");

    @Override
    protected Writeable.Reader<StartRelocationResponse> instanceReader() {
        return StartRelocationResponse::new;
    }

    @Override
    protected StartRelocationResponse createTestInstance() {
        return new StartRelocationResponse(randomMetrics());
    }

    @Override
    protected StartRelocationResponse mutateInstance(StartRelocationResponse instance) {
        return new StartRelocationResponse(mutateMetrics(instance.getRelocationSourceMetrics()));
    }

    public void testMetricsDroppedOnUnsupportedVersion() throws IOException {
        final TransportVersion version = TransportVersionUtils.randomVersionNotSupporting(METRICS_IN_RESPONSE);
        final StartRelocationResponse copy = copyWriteable(
            new StartRelocationResponse(randomMetrics()),
            getNamedWriteableRegistry(),
            instanceReader(),
            version
        );
        assertThat(copy.getRelocationSourceMetrics(), is(nullValue()));
    }

    private static RelocationSourceMetrics randomMetrics() {
        return new RelocationSourceMetrics(
            randomNonNegativeLong(),
            randomNonNegativeLong(),
            randomNonNegativeLong(),
            randomNonNegativeLong()
        );
    }

    private static RelocationSourceMetrics mutateMetrics(RelocationSourceMetrics instance) {
        return switch (randomIntBetween(0, 3)) {
            case 0 -> new RelocationSourceMetrics(
                randomValueOtherThan(instance.initialFlushDurationInMillis(), () -> randomNonNegativeLong()),
                instance.acquirePermitsDurationInMillis(),
                instance.secondFlushDurationInMillis(),
                instance.handoffDurationInMillis()
            );
            case 1 -> new RelocationSourceMetrics(
                instance.initialFlushDurationInMillis(),
                randomValueOtherThan(instance.acquirePermitsDurationInMillis(), () -> randomNonNegativeLong()),
                instance.secondFlushDurationInMillis(),
                instance.handoffDurationInMillis()
            );
            case 2 -> new RelocationSourceMetrics(
                instance.initialFlushDurationInMillis(),
                instance.acquirePermitsDurationInMillis(),
                randomValueOtherThan(instance.secondFlushDurationInMillis(), () -> randomNonNegativeLong()),
                instance.handoffDurationInMillis()
            );
            case 3 -> new RelocationSourceMetrics(
                instance.initialFlushDurationInMillis(),
                instance.acquirePermitsDurationInMillis(),
                instance.secondFlushDurationInMillis(),
                randomValueOtherThan(instance.handoffDurationInMillis(), () -> randomNonNegativeLong())
            );
            default -> throw new AssertionError("unreachable");
        };
    }
}
