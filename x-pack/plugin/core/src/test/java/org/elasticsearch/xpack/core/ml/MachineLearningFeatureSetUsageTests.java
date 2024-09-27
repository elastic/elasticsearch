/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.core.ml;

import org.elasticsearch.TransportVersion;
import org.elasticsearch.TransportVersions;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.core.Tuple;

import java.io.IOException;
import java.util.Collections;

public class MachineLearningFeatureSetUsageTests extends AbstractBWCWireSerializationTestCase<MachineLearningFeatureSetUsage> {
    @Override
    protected Writeable.Reader<MachineLearningFeatureSetUsage> instanceReader() {
        return MachineLearningFeatureSetUsage::new;
    }

    @Override
    protected MachineLearningFeatureSetUsage createTestInstance() {
        boolean enabled = randomBoolean();

        if (enabled == false) {
            return new MachineLearningFeatureSetUsage(
                randomBoolean(),
                enabled,
                Collections.emptyMap(),
                Collections.emptyMap(),
                Collections.emptyMap(),
                Collections.emptyMap(),
                Collections.emptyMap(),
                0
            );
        } else {
            return new MachineLearningFeatureSetUsage(
                randomBoolean(),
                enabled,
                randomMap(0, 4, () -> new Tuple<>(randomAlphaOfLength(4), randomAlphaOfLength(4))),
                randomMap(0, 4, () -> new Tuple<>(randomAlphaOfLength(4), randomAlphaOfLength(4))),
                randomMap(0, 4, () -> new Tuple<>(randomAlphaOfLength(4), randomAlphaOfLength(4))),
                randomMap(0, 4, () -> new Tuple<>(randomAlphaOfLength(4), randomAlphaOfLength(4))),
                randomMap(0, 4, () -> new Tuple<>(randomAlphaOfLength(4), randomAlphaOfLength(4))),
                randomIntBetween(1, 10)
            );
        }
    }

    @Override
    protected MachineLearningFeatureSetUsage mutateInstance(MachineLearningFeatureSetUsage instance) throws IOException {
        return null;
    }

    @Override
    protected MachineLearningFeatureSetUsage mutateInstanceForVersion(MachineLearningFeatureSetUsage instance, TransportVersion version) {
        if (version.before(TransportVersions.ML_TELEMETRY_MEMORY_ADDED)) {
            return new MachineLearningFeatureSetUsage(
                instance.available(),
                instance.enabled(),
                instance.getJobsUsage(),
                instance.getDatafeedsUsage(),
                instance.getAnalyticsUsage(),
                instance.getInferenceUsage(),
                Collections.emptyMap(),
                instance.getNodeCount()
            );
        }

        return instance;
    }
}
