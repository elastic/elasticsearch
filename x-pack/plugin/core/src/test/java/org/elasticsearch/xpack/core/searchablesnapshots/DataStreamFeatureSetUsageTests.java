/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.core.searchablesnapshots;

import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.test.AbstractWireSerializingTestCase;
import org.elasticsearch.xpack.core.datastreams.DataStreamFeatureSetUsage;

import java.io.IOException;

public class DataStreamFeatureSetUsageTests extends AbstractWireSerializingTestCase<DataStreamFeatureSetUsage> {

    @Override
    protected DataStreamFeatureSetUsage createTestInstance() {
        return new DataStreamFeatureSetUsage(new DataStreamFeatureSetUsage.DataStreamStats(randomNonNegativeLong(),
            randomNonNegativeLong()));
    }

    @Override
    protected DataStreamFeatureSetUsage mutateInstance(DataStreamFeatureSetUsage instance) throws IOException {
        return randomValueOtherThan(instance, this::createTestInstance);
    }

    @Override
    protected Writeable.Reader<DataStreamFeatureSetUsage> instanceReader() {
        return DataStreamFeatureSetUsage::new;
    }

}
