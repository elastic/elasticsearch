/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.core.searchablesnapshots;

import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.test.AbstractWireSerializingTestCase;

import java.io.IOException;

public class SearchableSnapshotsFeatureSetUsageTests extends AbstractWireSerializingTestCase<SearchableSnapshotFeatureSetUsage> {

    @Override
    protected SearchableSnapshotFeatureSetUsage createTestInstance() {
        boolean available = randomBoolean();
        return new SearchableSnapshotFeatureSetUsage(available, randomIntBetween(0, 100000), randomIntBetween(0, 100000));
    }

    @Override
    protected SearchableSnapshotFeatureSetUsage mutateInstance(SearchableSnapshotFeatureSetUsage instance) throws IOException {
        boolean available = instance.available();
        int numFullCopySearchableSnapshotIndices = instance.getNumberOfFullCopySearchableSnapshotIndices();
        int numSharedCacheSearchableSnapshotIndices = instance.getNumberOfSharedCacheSearchableSnapshotIndices();
        switch (between(0, 2)) {
            case 0 -> available = available == false;
            case 1 -> numFullCopySearchableSnapshotIndices = randomValueOtherThan(
                numFullCopySearchableSnapshotIndices,
                () -> randomIntBetween(0, 100000)
            );
            case 2 -> numSharedCacheSearchableSnapshotIndices = randomValueOtherThan(
                numSharedCacheSearchableSnapshotIndices,
                () -> randomIntBetween(0, 100000)
            );
            default -> throw new AssertionError("Illegal randomisation branch");
        }
        return new SearchableSnapshotFeatureSetUsage(
            available,
            numFullCopySearchableSnapshotIndices,
            numSharedCacheSearchableSnapshotIndices
        );
    }

    @Override
    protected Writeable.Reader<SearchableSnapshotFeatureSetUsage> instanceReader() {
        return SearchableSnapshotFeatureSetUsage::new;
    }

}
