/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.snapshotlifecycle;

import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.test.AbstractSerializingTestCase;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

public class SnapshotLifecyclePolicyTests extends AbstractSerializingTestCase<SnapshotLifecyclePolicy> {

    private String id;

    @Override
    protected SnapshotLifecyclePolicy doParseInstance(XContentParser parser) throws IOException {
        return SnapshotLifecyclePolicy.parse(parser, id);
    }

    @Override
    protected SnapshotLifecyclePolicy createTestInstance() {
        Map<String, Object> config = new HashMap<>();
        for (int i = 0; i < randomIntBetween(2, 5); i++) {
            config.put(randomAlphaOfLength(4), randomAlphaOfLength(4));
        }
        id = randomAlphaOfLength(5);
        return new SnapshotLifecyclePolicy(id,
            randomAlphaOfLength(4),
            randomSchedule(),
            randomAlphaOfLength(4),
            config);
    }

    private String randomSchedule() {
        return randomIntBetween(0, 59) + " " +
            randomIntBetween(0, 59) + " " +
            randomIntBetween(0, 12) + " * * ?";
    }

    @Override
    protected SnapshotLifecyclePolicy mutateInstance(SnapshotLifecyclePolicy instance) throws IOException {
        switch (between(0, 4)) {
            case 0:
                return new SnapshotLifecyclePolicy(instance.getId() + randomAlphaOfLength(2),
                    instance.getName(),
                    instance.getSchedule(),
                    instance.getRepository(),
                    instance.getConfig());
            case 1:
                return new SnapshotLifecyclePolicy(instance.getId(),
                    instance.getName() + randomAlphaOfLength(2),
                    instance.getSchedule(),
                    instance.getRepository(),
                    instance.getConfig());
            case 2:
                return new SnapshotLifecyclePolicy(instance.getId(),
                    instance.getName(),
                    randomValueOtherThan(instance.getSchedule(), this::randomSchedule),
                    instance.getRepository(),
                    instance.getConfig());
            case 3:
                return new SnapshotLifecyclePolicy(instance.getId(),
                    instance.getName(),
                    instance.getSchedule(),
                    instance.getRepository() + randomAlphaOfLength(2),
                    instance.getConfig());
            case 4:
                Map<String, Object> newConfig = new HashMap<>();
                for (int i = 0; i < randomIntBetween(2, 5); i++) {
                    newConfig.put(randomAlphaOfLength(3), randomAlphaOfLength(3));
                }
                return new SnapshotLifecyclePolicy(instance.getId(),
                    instance.getName() + randomAlphaOfLength(2),
                    instance.getSchedule(),
                    instance.getRepository(),
                    newConfig);
            default:
                throw new AssertionError("failure, got illegal switch case");
        }
    }

    @Override
    protected Writeable.Reader<SnapshotLifecyclePolicy> instanceReader() {
        return SnapshotLifecyclePolicy::new;
    }
}
