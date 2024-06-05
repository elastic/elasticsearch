/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.cluster.service;

import org.elasticsearch.cluster.service.ClusterApplierRecordingService.Stats;
import org.elasticsearch.cluster.service.ClusterApplierRecordingService.Stats.Recording;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.util.Maps;
import org.elasticsearch.test.AbstractWireSerializingTestCase;

import java.util.Map;

public class ClusterApplierRecordingServiceStatsTests extends AbstractWireSerializingTestCase<Stats> {

    @Override
    protected Writeable.Reader<Stats> instanceReader() {
        return Stats::new;
    }

    @Override
    protected Stats createTestInstance() {
        int numRecordings = randomInt(256);
        Map<String, Recording> recordings = Maps.newMapWithExpectedSize(numRecordings);
        for (int i = 0; i < numRecordings; i++) {
            recordings.put(randomAlphaOfLength(16), new Recording(randomNonNegativeLong(), randomNonNegativeLong()));
        }
        return new Stats(recordings);
    }

    @Override
    protected Stats mutateInstance(Stats instance) {
        return null;// TODO implement https://github.com/elastic/elasticsearch/issues/25929
    }
}
