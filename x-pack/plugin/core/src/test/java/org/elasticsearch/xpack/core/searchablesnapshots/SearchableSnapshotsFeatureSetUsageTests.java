/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.core.searchablesnapshots;

import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.test.AbstractWireSerializingTestCase;

import java.io.IOException;

public class SearchableSnapshotsFeatureSetUsageTests extends AbstractWireSerializingTestCase<SearchableSnapshotFeatureSetUsage> {

    @Override
    protected SearchableSnapshotFeatureSetUsage createTestInstance() {
        boolean available = randomBoolean();
        boolean enabled = randomBoolean();
        return new SearchableSnapshotFeatureSetUsage(available, enabled, randomIntBetween(0, 100000));
    }

    @Override
    protected SearchableSnapshotFeatureSetUsage mutateInstance(SearchableSnapshotFeatureSetUsage instance) throws IOException {
        boolean available = instance.available();
        boolean enabled = instance.enabled();
        int numSearchableSnapshotIndices = instance.getNumberOfSearchableSnapshotIndices();
        switch (between(0, 2)) {
            case 0:
                available = available == false;
                break;
            case 1:
                enabled = enabled == false;
                break;
            case 2:
                numSearchableSnapshotIndices = randomValueOtherThan(numSearchableSnapshotIndices, () -> randomIntBetween(0, 100000));
                break;
            default:
                throw new AssertionError("Illegal randomisation branch");
        }
        return new SearchableSnapshotFeatureSetUsage(available, enabled, numSearchableSnapshotIndices);
    }

    @Override
    protected Writeable.Reader<SearchableSnapshotFeatureSetUsage> instanceReader() {
        return SearchableSnapshotFeatureSetUsage::new;
    }

}
