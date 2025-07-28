/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.core.archive;

import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.test.AbstractWireSerializingTestCase;

public class ArchiveFeatureSetUsageTests extends AbstractWireSerializingTestCase<ArchiveFeatureSetUsage> {

    @Override
    protected ArchiveFeatureSetUsage createTestInstance() {
        boolean available = randomBoolean();
        return new ArchiveFeatureSetUsage(available, randomIntBetween(0, 100000));
    }

    @Override
    protected ArchiveFeatureSetUsage mutateInstance(ArchiveFeatureSetUsage instance) {
        boolean available = instance.available();
        int numArchiveIndices = instance.getNumberOfArchiveIndices();
        switch (between(0, 1)) {
            case 0 -> available = available == false;
            case 1 -> numArchiveIndices = randomValueOtherThan(numArchiveIndices, () -> randomIntBetween(0, 100000));
            default -> throw new AssertionError("Illegal randomisation branch");
        }
        return new ArchiveFeatureSetUsage(available, numArchiveIndices);
    }

    @Override
    protected Writeable.Reader<ArchiveFeatureSetUsage> instanceReader() {
        return ArchiveFeatureSetUsage::new;
    }

}
