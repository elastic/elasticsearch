/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.core.application;

import org.elasticsearch.TransportVersion;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.test.AbstractWireSerializingTestCase;

import java.io.IOException;

import static org.hamcrest.Matchers.equalTo;

public class LogsDBColumnarFeatureSetUsageTests extends AbstractWireSerializingTestCase<LogsDBColumnarFeatureSetUsage> {

    @Override
    protected Writeable.Reader<LogsDBColumnarFeatureSetUsage> instanceReader() {
        return LogsDBColumnarFeatureSetUsage::new;
    }

    @Override
    protected LogsDBColumnarFeatureSetUsage createTestInstance() {
        return new LogsDBColumnarFeatureSetUsage(
            randomBoolean(),
            randomBoolean(),
            randomIntBetween(0, 1000),
            randomIntBetween(0, 1000),
            randomNonNegativeLong(),
            randomNonNegativeLong(),
            randomIntBetween(0, 1000),
            randomIntBetween(0, 1000),
            randomIntBetween(0, 1000)
        );
    }

    @Override
    protected LogsDBColumnarFeatureSetUsage mutateInstance(LogsDBColumnarFeatureSetUsage instance) throws IOException {
        boolean available = instance.available();
        boolean enabled = instance.enabled();
        int indicesCount = instance.indicesCount();
        int indicesWithSyntheticSource = instance.indicesWithSyntheticSource();
        long numDocs = instance.numDocs();
        long sizeInBytes = instance.sizeInBytes();
        int dataStreamsCount = instance.dataStreamsCount();
        int dataStreamsManagedByIlm = instance.dataStreamsManagedByIlm();
        int dataStreamsManagedByDlm = instance.dataStreamsManagedByDlm();
        return switch (between(0, 8)) {
            case 0 -> new LogsDBColumnarFeatureSetUsage(
                available == false,
                enabled,
                indicesCount,
                indicesWithSyntheticSource,
                numDocs,
                sizeInBytes,
                dataStreamsCount,
                dataStreamsManagedByIlm,
                dataStreamsManagedByDlm
            );
            case 1 -> new LogsDBColumnarFeatureSetUsage(
                available,
                enabled == false,
                indicesCount,
                indicesWithSyntheticSource,
                numDocs,
                sizeInBytes,
                dataStreamsCount,
                dataStreamsManagedByIlm,
                dataStreamsManagedByDlm
            );
            case 2 -> new LogsDBColumnarFeatureSetUsage(
                available,
                enabled,
                randomValueOtherThan(indicesCount, () -> randomIntBetween(0, 1000)),
                indicesWithSyntheticSource,
                numDocs,
                sizeInBytes,
                dataStreamsCount,
                dataStreamsManagedByIlm,
                dataStreamsManagedByDlm
            );
            case 3 -> new LogsDBColumnarFeatureSetUsage(
                available,
                enabled,
                indicesCount,
                randomValueOtherThan(indicesWithSyntheticSource, () -> randomIntBetween(0, 1000)),
                numDocs,
                sizeInBytes,
                dataStreamsCount,
                dataStreamsManagedByIlm,
                dataStreamsManagedByDlm
            );
            case 4 -> new LogsDBColumnarFeatureSetUsage(
                available,
                enabled,
                indicesCount,
                indicesWithSyntheticSource,
                randomValueOtherThan(numDocs, () -> randomNonNegativeLong()),
                sizeInBytes,
                dataStreamsCount,
                dataStreamsManagedByIlm,
                dataStreamsManagedByDlm
            );
            case 5 -> new LogsDBColumnarFeatureSetUsage(
                available,
                enabled,
                indicesCount,
                indicesWithSyntheticSource,
                numDocs,
                randomValueOtherThan(sizeInBytes, () -> randomNonNegativeLong()),
                dataStreamsCount,
                dataStreamsManagedByIlm,
                dataStreamsManagedByDlm
            );
            case 6 -> new LogsDBColumnarFeatureSetUsage(
                available,
                enabled,
                indicesCount,
                indicesWithSyntheticSource,
                numDocs,
                sizeInBytes,
                randomValueOtherThan(dataStreamsCount, () -> randomIntBetween(0, 1000)),
                dataStreamsManagedByIlm,
                dataStreamsManagedByDlm
            );
            case 7 -> new LogsDBColumnarFeatureSetUsage(
                available,
                enabled,
                indicesCount,
                indicesWithSyntheticSource,
                numDocs,
                sizeInBytes,
                dataStreamsCount,
                randomValueOtherThan(dataStreamsManagedByIlm, () -> randomIntBetween(0, 1000)),
                dataStreamsManagedByDlm
            );
            case 8 -> new LogsDBColumnarFeatureSetUsage(
                available,
                enabled,
                indicesCount,
                indicesWithSyntheticSource,
                numDocs,
                sizeInBytes,
                dataStreamsCount,
                dataStreamsManagedByIlm,
                randomValueOtherThan(dataStreamsManagedByDlm, () -> randomIntBetween(0, 1000))
            );
            default -> throw new AssertionError("unexpected branch");
        };
    }

    public void testGetMinimalSupportedVersion() {
        assertThat(createTestInstance().getMinimalSupportedVersion(), equalTo(TransportVersion.fromName("logsdb_columnar_usage")));
    }
}
