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
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xpack.core.slm.SnapshotLifecyclePolicy;
import org.elasticsearch.xpack.core.slm.SnapshotLifecyclePolicyMetadataTests;
import org.elasticsearch.xpack.core.slm.SnapshotRetentionConfiguration;

import java.io.IOException;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneOffset;
import java.time.temporal.ChronoUnit;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import static org.elasticsearch.xpack.core.slm.SnapshotLifecyclePolicyMetadataTests.randomSnapshotLifecyclePolicy;
import static org.elasticsearch.xpack.core.slm.SnapshotLifecyclePolicyMetadataTests.randomTimeValueString;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.nullValue;

public class SnapshotLifecyclePolicyTests extends AbstractXContentSerializingTestCase<SnapshotLifecyclePolicy> {

    private String id;

    public void testToRequest() {
        var schedule = randomBoolean() ? "0 1 2 3 4 ? 2099" : "30m";
        SnapshotLifecyclePolicy p = new SnapshotLifecyclePolicy(
            "id",
            "name",
            schedule,
            "repo",
            Collections.emptyMap(),
            SnapshotRetentionConfiguration.EMPTY,
            null
        );
        CreateSnapshotRequest request = p.toRequest(TEST_REQUEST_TIMEOUT);
        CreateSnapshotRequest expected = new CreateSnapshotRequest(TEST_REQUEST_TIMEOUT).userMetadata(
            Collections.singletonMap("policy", "id")
        );

        p = new SnapshotLifecyclePolicy("id", "name", schedule, "repo", null, null, null);
        request = p.toRequest(TEST_REQUEST_TIMEOUT);
        expected.waitForCompletion(true).snapshot(request.snapshot()).repository("repo").uuid(request.uuid());
        assertEquals(expected, request);
    }

    public void testNextExecutionTimeSchedule() {
        SnapshotLifecyclePolicy p = new SnapshotLifecyclePolicy(
            "id",
            "name",
            "0 1 2 3 4 ? 2049",
            "repo",
            Collections.emptyMap(),
            SnapshotRetentionConfiguration.EMPTY,
            null
        );
        assertThat(p.calculateNextExecution(-1, Clock.systemUTC()), equalTo(2501028060000L));
    }

    public void testNextExecutionTimeInterval() {
        SnapshotLifecyclePolicy p = new SnapshotLifecyclePolicy(
            "id",
            "name",
            "30m",
            "repo",
            Collections.emptyMap(),
            SnapshotRetentionConfiguration.EMPTY,
            null
        );

        {
            // current time is exactly modified time
            Instant modifiedTime = Instant.parse("2024-07-17T00:00:00.000Z").truncatedTo(ChronoUnit.SECONDS);
            Instant currentTime = modifiedTime;
            Instant expected = Instant.parse("2024-07-17T00:30:00.000Z").truncatedTo(ChronoUnit.SECONDS);
            assertThat(p.calculateNextExecution(modifiedTime.toEpochMilli(), fixedClock(currentTime)), equalTo(expected.toEpochMilli()));
        }

        {
            // current time is half an interval past modified time
            Instant modifiedTime = Instant.parse("2024-07-17T00:00:00.000Z").truncatedTo(ChronoUnit.SECONDS);
            Instant currentTime = modifiedTime.plus(Duration.ofMinutes(15));
            Instant expected = Instant.parse("2024-07-17T00:30:00.000Z").truncatedTo(ChronoUnit.SECONDS);
            assertThat(p.calculateNextExecution(modifiedTime.toEpochMilli(), fixedClock(currentTime)), equalTo(expected.toEpochMilli()));
        }

        {
            // current time is a full day (24 intervals) ahead of modified time
            Instant modifiedTime = Instant.parse("2024-07-17T00:00:00.000Z").truncatedTo(ChronoUnit.SECONDS);
            Instant currentTime = modifiedTime.plus(Duration.ofDays(1));
            Instant expected = Instant.parse("2024-07-18T00:30:00.000Z").truncatedTo(ChronoUnit.SECONDS);
            assertThat(p.calculateNextExecution(modifiedTime.toEpochMilli(), fixedClock(currentTime)), equalTo(expected.toEpochMilli()));
        }

        {
            // current time before modified time
            Instant modifiedTime = Instant.parse("2024-07-17T00:00:00.000Z").truncatedTo(ChronoUnit.SECONDS);
            Instant currentTime = modifiedTime.minus(Duration.ofHours(1));
            expectThrows(AssertionError.class, () -> p.calculateNextExecution(modifiedTime.toEpochMilli(), fixedClock(currentTime)));
        }

        {
            // current time is every minute of a day
            Instant modifiedTime = Instant.parse("2024-07-17T00:00:00.000Z").truncatedTo(ChronoUnit.SECONDS);
            Instant currentTime = modifiedTime;
            Instant expectedTime = modifiedTime.plus(Duration.ofMinutes(30));

            for (; currentTime.isBefore(modifiedTime.plus(Duration.ofDays(1))); currentTime = currentTime.plus(Duration.ofMinutes(1))) {
                if (currentTime.equals(expectedTime)) {
                    expectedTime = expectedTime.plus(Duration.ofMinutes(30));
                }
                assertThat(
                    p.calculateNextExecution(modifiedTime.toEpochMilli(), fixedClock(currentTime)),
                    equalTo(expectedTime.toEpochMilli())
                );
            }
        }
    }

    private static Clock fixedClock(Instant instant) {
        return Clock.fixed(instant, ZoneOffset.UTC);
    }

    public void testCalculateNextIntervalInterval() {

        {
            SnapshotLifecyclePolicy p = new SnapshotLifecyclePolicy(
                "id",
                "name",
                "30m",
                "repo",
                Collections.emptyMap(),
                SnapshotRetentionConfiguration.EMPTY,
                null
            );
            assertThat(p.calculateNextInterval(Clock.systemUTC()), equalTo(TimeValue.timeValueMinutes(30)));
        }
        {
            String schedule = randomTimeValueString();
            SnapshotLifecyclePolicy p = new SnapshotLifecyclePolicy(
                "id",
                "name",
                schedule,
                "repo",
                Collections.emptyMap(),
                SnapshotRetentionConfiguration.EMPTY,
                null
            );
            assertThat(p.calculateNextInterval(Clock.systemUTC()), equalTo(TimeValue.parseTimeValue(schedule, "schedule")));
        }
    }

    public void testCalculateNextIntervalSchedule() {
        {
            SnapshotLifecyclePolicy p = new SnapshotLifecyclePolicy(
                "id",
                "name",
                "0 0/5 * * * ?",
                "repo",
                Collections.emptyMap(),
                SnapshotRetentionConfiguration.EMPTY,
                null
            );
            assertThat(p.calculateNextInterval(Clock.systemUTC()), equalTo(TimeValue.timeValueMinutes(5)));
        }

        {
            SnapshotLifecyclePolicy p = new SnapshotLifecyclePolicy(
                "id",
                "name",
                "0 1 2 3 4 ? 2099",
                "repo",
                Collections.emptyMap(),
                SnapshotRetentionConfiguration.EMPTY,
                null
            );
            assertThat(p.calculateNextInterval(Clock.systemUTC()), equalTo(TimeValue.MINUS_ONE));
        }

        {
            SnapshotLifecyclePolicy p = new SnapshotLifecyclePolicy(
                "id",
                "name",
                "* * * 31 FEB ? *",
                "repo",
                Collections.emptyMap(),
                SnapshotRetentionConfiguration.EMPTY,
                null
            );
            assertThat(p.calculateNextInterval(Clock.systemUTC()), equalTo(TimeValue.MINUS_ONE));
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
                SnapshotRetentionConfiguration.EMPTY,
                null
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
                    "invalid schedule [* * * * * L]: must be a valid cron expression or time unit"
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
                SnapshotRetentionConfiguration.EMPTY,
                null
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
                "0d",
                "repo",
                Collections.emptyMap(),
                SnapshotRetentionConfiguration.EMPTY,
                null
            );

            ValidationException e = policy.validate();
            assertThat(e.validationErrors(), containsInAnyOrder("invalid schedule [0d]: time unit must be at least 1 millisecond"));
        }

        {
            SnapshotLifecyclePolicy policy = new SnapshotLifecyclePolicy(
                "my_policy",
                "my_snap",
                "999micros",
                "repo",
                Collections.emptyMap(),
                SnapshotRetentionConfiguration.EMPTY,
                null
            );

            ValidationException e = policy.validate();
            assertThat(e.validationErrors(), containsInAnyOrder("invalid schedule [999micros]: time unit must be at least 1 millisecond"));
        }

        {
            SnapshotLifecyclePolicy policy = new SnapshotLifecyclePolicy(
                "my_policy",
                "my_snap",
                "0 0/30 * * * ?",
                "repo",
                Collections.emptyMap(),
                SnapshotRetentionConfiguration.EMPTY,
                null
            );
            ValidationException e = policy.validate();
            assertThat(e, nullValue());
        }

        {
            SnapshotLifecyclePolicy policy = new SnapshotLifecyclePolicy(
                "my_policy",
                "my_snap",
                "30m",
                "repo",
                Collections.emptyMap(),
                SnapshotRetentionConfiguration.EMPTY,
                TimeValue.parseTimeValue("1h", "unhealthyIfNoSnapshotWithin")
            );
            ValidationException e = policy.validate();
            assertThat(e, nullValue());
        }

        {
            SnapshotLifecyclePolicy policy = new SnapshotLifecyclePolicy(
                "my_policy",
                "my_snap",
                "1ms",
                "repo",
                Collections.emptyMap(),
                SnapshotRetentionConfiguration.EMPTY,
                null
            );

            ValidationException e = policy.validate();
            assertThat(e, nullValue());
        }
        {
            SnapshotLifecyclePolicy policy = new SnapshotLifecyclePolicy(
                "my_policy",
                "my_snap",
                "15m",
                "repo",
                Collections.emptyMap(),
                SnapshotRetentionConfiguration.EMPTY,
                TimeValue.ONE_MINUTE
            );

            ValidationException e = policy.validate();
            assertThat(
                e.validationErrors(),
                containsInAnyOrder(
                    "invalid unhealthy_if_no_snapshot_within [1m]: time is too short, "
                        + "expecting at least more than the interval between snapshots [15m] for schedule [15m]"
                )
            );
        }
        {
            SnapshotLifecyclePolicy policy = new SnapshotLifecyclePolicy(
                "my_policy",
                "my_snap",
                "0 0 1 * * ?",  // every day
                "repo",
                Collections.emptyMap(),
                SnapshotRetentionConfiguration.EMPTY,
                TimeValue.parseTimeValue("2d", "unhealthyIfNoSnapshotWithin")
            );

            ValidationException e = policy.validate();
            assertThat(e, nullValue());
        }
        {
            SnapshotLifecyclePolicy policy = new SnapshotLifecyclePolicy(
                "my_policy",
                "my_snap",
                "0 0 1 * * ?",  // every day
                "repo",
                Collections.emptyMap(),
                SnapshotRetentionConfiguration.EMPTY,
                TimeValue.ONE_MINUTE
            );

            ValidationException e = policy.validate();
            assertThat(
                e.validationErrors(),
                containsInAnyOrder(
                    "invalid unhealthy_if_no_snapshot_within [1m]: time is too short, "
                        + "expecting at least more than the interval between snapshots [1d] for schedule [0 0 1 * * ?]"
                )
            );

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
                SnapshotRetentionConfiguration.EMPTY,
                null
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
                SnapshotRetentionConfiguration.EMPTY,
                null
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
                SnapshotRetentionConfiguration.EMPTY,
                null
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
        String id = instance.getId();
        String name = instance.getName();
        String schedule = instance.getSchedule();
        String repository = instance.getRepository();
        Map<String, Object> config = instance.getConfig();
        SnapshotRetentionConfiguration retentionPolicy = instance.getRetentionPolicy();
        TimeValue unhealthyIfNoSnapshotWithin = instance.getUnhealthyIfNoSnapshotWithin();

        switch (between(0, 6)) {
            case 0 -> id += randomAlphaOfLength(2);
            case 1 -> name += randomAlphaOfLength(2);
            case 2 -> schedule = randomValueOtherThan(instance.getSchedule(), SnapshotLifecyclePolicyMetadataTests::randomSchedule);
            case 3 -> repository += randomAlphaOfLength(2);
            case 4 -> {
                Map<String, Object> newConfig = new HashMap<>();
                for (int i = 0; i < randomIntBetween(2, 5); i++) {
                    newConfig.put(randomAlphaOfLength(3), randomAlphaOfLength(3));
                }
                config = newConfig;
            }
            case 5 -> retentionPolicy = randomValueOtherThan(
                instance.getRetentionPolicy(),
                SnapshotLifecyclePolicyMetadataTests::randomRetention
            );
            case 6 -> unhealthyIfNoSnapshotWithin = randomValueOtherThan(
                instance.getUnhealthyIfNoSnapshotWithin(),
                ESTestCase::randomTimeValue
            );
            default -> throw new AssertionError("failure, got illegal switch case");
        }
        return new SnapshotLifecyclePolicy(id, name, schedule, repository, config, retentionPolicy, unhealthyIfNoSnapshotWithin);
    }

    @Override
    protected Writeable.Reader<SnapshotLifecyclePolicy> instanceReader() {
        return SnapshotLifecyclePolicy::new;
    }
}
