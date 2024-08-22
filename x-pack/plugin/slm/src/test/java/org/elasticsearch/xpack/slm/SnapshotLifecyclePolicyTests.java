/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.slm;

import org.elasticsearch.action.admin.cluster.snapshots.create.CreateSnapshotRequest;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.ValidationException;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.test.AbstractXContentSerializingTestCase;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xpack.core.slm.SnapshotLifecyclePolicy;
import org.elasticsearch.xpack.core.slm.SnapshotLifecyclePolicyMetadataTests;
import org.elasticsearch.xpack.core.slm.SnapshotRetentionConfiguration;

import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import static org.elasticsearch.xpack.core.slm.SnapshotLifecyclePolicyMetadataTests.randomSnapshotLifecyclePolicy;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.nullValue;

public class SnapshotLifecyclePolicyTests extends AbstractXContentSerializingTestCase<SnapshotLifecyclePolicy> {

    private String id;

    public void testToRequest() {
        SnapshotLifecyclePolicy p = new SnapshotLifecyclePolicy(
            "id",
            "name",
            "0 1 2 3 4 ? 2099",
            "repo",
            Collections.emptyMap(),
            SnapshotRetentionConfiguration.EMPTY
        );
        CreateSnapshotRequest request = p.toRequest(TEST_REQUEST_TIMEOUT);
        CreateSnapshotRequest expected = new CreateSnapshotRequest(TEST_REQUEST_TIMEOUT).userMetadata(
            Collections.singletonMap("policy", "id")
        );

        p = new SnapshotLifecyclePolicy("id", "name", "0 1 2 3 4 ? 2099", "repo", null, null);
        request = p.toRequest(TEST_REQUEST_TIMEOUT);
        expected.waitForCompletion(true).snapshot(request.snapshot()).repository("repo").uuid(request.uuid());
        assertEquals(expected, request);
    }

    public void testNextExecutionTime() {
        SnapshotLifecyclePolicy p = new SnapshotLifecyclePolicy(
            "id",
            "name",
            "0 1 2 3 4 ? 2099",
            "repo",
            Collections.emptyMap(),
            SnapshotRetentionConfiguration.EMPTY
        );
        assertThat(p.calculateNextExecution(), equalTo(4078864860000L));
    }

    public void testCalculateNextInterval() {
        {
            SnapshotLifecyclePolicy p = new SnapshotLifecyclePolicy(
                "id",
                "name",
                "0 0/5 * * * ?",
                "repo",
                Collections.emptyMap(),
                SnapshotRetentionConfiguration.EMPTY
            );
            assertThat(p.calculateNextInterval(), equalTo(TimeValue.timeValueMinutes(5)));
        }

        {
            SnapshotLifecyclePolicy p = new SnapshotLifecyclePolicy(
                "id",
                "name",
                "0 1 2 3 4 ? 2099",
                "repo",
                Collections.emptyMap(),
                SnapshotRetentionConfiguration.EMPTY
            );
            assertThat(p.calculateNextInterval(), equalTo(TimeValue.MINUS_ONE));
        }

        {
            SnapshotLifecyclePolicy p = new SnapshotLifecyclePolicy(
                "id",
                "name",
                "* * * 31 FEB ? *",
                "repo",
                Collections.emptyMap(),
                SnapshotRetentionConfiguration.EMPTY
            );
            assertThat(p.calculateNextInterval(), equalTo(TimeValue.MINUS_ONE));
        }
    }

    public void testValidation() {
        {
            SnapshotLifecyclePolicy policy = new SnapshotLifecyclePolicy(
                "a,b",
                "<my, snapshot-{now/M}>",
                "* * * * * L",
                "  ",
                Collections.emptyMap(),
                SnapshotRetentionConfiguration.EMPTY
            );

            ValidationException e = policy.validate();
            assertThat(
                e.validationErrors(),
                containsInAnyOrder(
                    "invalid policy id [a,b]: must not contain the following characters " + Strings.INVALID_FILENAME_CHARS,
                    "invalid snapshot name [<my, snapshot-{now/M}>]: must not contain contain"
                        + " the following characters "
                        + Strings.INVALID_FILENAME_CHARS,
                    "invalid repository name [  ]: cannot be empty",
                    "invalid schedule: invalid cron expression [* * * * * L]"
                )
            );
        }

        {
            SnapshotLifecyclePolicy policy = new SnapshotLifecyclePolicy(
                "_my_policy",
                "mySnap",
                " ",
                "repo",
                Collections.emptyMap(),
                SnapshotRetentionConfiguration.EMPTY
            );

            ValidationException e = policy.validate();
            assertThat(
                e.validationErrors(),
                containsInAnyOrder(
                    "invalid policy id [_my_policy]: must not start with '_'",
                    "invalid snapshot name [mySnap]: must be lowercase",
                    "invalid schedule [ ]: must not be empty"
                )
            );
        }

        {
            SnapshotLifecyclePolicy policy = new SnapshotLifecyclePolicy(
                "my_policy",
                "my_snap",
                "0 0/30 * * * ?",
                "repo",
                Collections.emptyMap(),
                SnapshotRetentionConfiguration.EMPTY
            );
            ValidationException e = policy.validate();
            assertThat(e, nullValue());
        }
    }

    public void testMetadataValidation() {
        {
            Map<String, Object> configuration = new HashMap<>();
            final String metadataString = randomAlphaOfLength(10);
            configuration.put("metadata", metadataString);

            SnapshotLifecyclePolicy policy = new SnapshotLifecyclePolicy(
                "mypolicy",
                "<mysnapshot-{now/M}>",
                "1 * * * * ?",
                "myrepo",
                configuration,
                SnapshotRetentionConfiguration.EMPTY
            );
            ValidationException e = policy.validate();
            assertThat(
                e.validationErrors(),
                contains("invalid configuration.metadata [" + metadataString + "]: must be an object if present")
            );
        }

        {
            Map<String, Object> metadata = new HashMap<>();
            metadata.put("policy", randomAlphaOfLength(5));
            Map<String, Object> configuration = new HashMap<>();
            configuration.put("metadata", metadata);

            SnapshotLifecyclePolicy policy = new SnapshotLifecyclePolicy(
                "mypolicy",
                "<mysnapshot-{now/M}>",
                "1 * * * * ?",
                "myrepo",
                configuration,
                SnapshotRetentionConfiguration.EMPTY
            );
            ValidationException e = policy.validate();
            assertThat(
                e.validationErrors(),
                contains("invalid configuration.metadata: field name [policy] is reserved and " + "will be added automatically")
            );
        }

        {
            Map<String, Object> metadata = new HashMap<>();
            final int fieldCount = randomIntBetween(67, 100); // 67 is the smallest field count with these sizes that causes an error
            final int keyBytes = 5; // chosen arbitrarily
            final int valueBytes = 4; // chosen arbitrarily
            int totalBytes = fieldCount * (keyBytes + valueBytes + 6 /* bytes of overhead per key/value pair */) + 1;
            for (int i = 0; i < fieldCount; i++) {
                metadata.put(
                    randomValueOtherThanMany(key -> "policy".equals(key) || metadata.containsKey(key), () -> randomAlphaOfLength(keyBytes)),
                    randomAlphaOfLength(valueBytes)
                );
            }
            Map<String, Object> configuration = new HashMap<>();
            configuration.put("metadata", metadata);

            SnapshotLifecyclePolicy policy = new SnapshotLifecyclePolicy(
                "mypolicy",
                "<mysnapshot-{now/M}>",
                "1 * * * * ?",
                "myrepo",
                configuration,
                SnapshotRetentionConfiguration.EMPTY
            );
            ValidationException e = policy.validate();
            assertThat(
                e.validationErrors(),
                contains("invalid configuration.metadata: must be smaller than [1004] bytes, but is [" + totalBytes + "] bytes")
            );
        }
    }

    @Override
    protected SnapshotLifecyclePolicy doParseInstance(XContentParser parser) throws IOException {
        return SnapshotLifecyclePolicy.parse(parser, id);
    }

    @Override
    protected SnapshotLifecyclePolicy createTestInstance() {
        id = randomAlphaOfLength(5);
        return randomSnapshotLifecyclePolicy(id);
    }

    @Override
    protected SnapshotLifecyclePolicy mutateInstance(SnapshotLifecyclePolicy instance) {
        switch (between(0, 5)) {
            case 0:
                return new SnapshotLifecyclePolicy(
                    instance.getId() + randomAlphaOfLength(2),
                    instance.getName(),
                    instance.getSchedule(),
                    instance.getRepository(),
                    instance.getConfig(),
                    instance.getRetentionPolicy()
                );
            case 1:
                return new SnapshotLifecyclePolicy(
                    instance.getId(),
                    instance.getName() + randomAlphaOfLength(2),
                    instance.getSchedule(),
                    instance.getRepository(),
                    instance.getConfig(),
                    instance.getRetentionPolicy()
                );
            case 2:
                return new SnapshotLifecyclePolicy(
                    instance.getId(),
                    instance.getName(),
                    randomValueOtherThan(instance.getSchedule(), SnapshotLifecyclePolicyMetadataTests::randomSchedule),
                    instance.getRepository(),
                    instance.getConfig(),
                    instance.getRetentionPolicy()
                );
            case 3:
                return new SnapshotLifecyclePolicy(
                    instance.getId(),
                    instance.getName(),
                    instance.getSchedule(),
                    instance.getRepository() + randomAlphaOfLength(2),
                    instance.getConfig(),
                    instance.getRetentionPolicy()
                );
            case 4:
                Map<String, Object> newConfig = new HashMap<>();
                for (int i = 0; i < randomIntBetween(2, 5); i++) {
                    newConfig.put(randomAlphaOfLength(3), randomAlphaOfLength(3));
                }
                return new SnapshotLifecyclePolicy(
                    instance.getId(),
                    instance.getName() + randomAlphaOfLength(2),
                    instance.getSchedule(),
                    instance.getRepository(),
                    newConfig,
                    instance.getRetentionPolicy()
                );
            case 5:
                return new SnapshotLifecyclePolicy(
                    instance.getId(),
                    instance.getName(),
                    instance.getSchedule(),
                    instance.getRepository(),
                    instance.getConfig(),
                    randomValueOtherThan(instance.getRetentionPolicy(), SnapshotLifecyclePolicyMetadataTests::randomRetention)
                );
            default:
                throw new AssertionError("failure, got illegal switch case");
        }
    }

    @Override
    protected Writeable.Reader<SnapshotLifecyclePolicy> instanceReader() {
        return SnapshotLifecyclePolicy::new;
    }
}
