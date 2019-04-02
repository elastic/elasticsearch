/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.snapshotlifecycle;

import org.elasticsearch.common.ValidationException;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.test.AbstractSerializingTestCase;
import org.elasticsearch.xpack.core.snapshotlifecycle.SnapshotLifecyclePolicy;

import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.startsWith;

public class SnapshotLifecyclePolicyTests extends AbstractSerializingTestCase<SnapshotLifecyclePolicy> {

    private String id;

    public void testNameGeneration() {
        long time = 1552684146542L; // Fri Mar 15 2019 21:09:06 UTC
        SnapshotLifecyclePolicy.ResolverContext context = new SnapshotLifecyclePolicy.ResolverContext(time);
        SnapshotLifecyclePolicy p = new SnapshotLifecyclePolicy("id", "name", "1 * * * * ?", "repo", Collections.emptyMap());
        assertThat(p.generateSnapshotName(context), startsWith("name-"));
        assertThat(p.generateSnapshotName(context).length(), greaterThan("name-".length()));

        p = new SnapshotLifecyclePolicy("id", "<name-{now}>", "1 * * * * ?", "repo", Collections.emptyMap());
        assertThat(p.generateSnapshotName(context), startsWith("name-2019.03.15-"));
        assertThat(p.generateSnapshotName(context).length(), greaterThan("name-2019.03.15-".length()));

        p = new SnapshotLifecyclePolicy("id", "<name-{now/M}>", "1 * * * * ?", "repo", Collections.emptyMap());
        assertThat(p.generateSnapshotName(context), startsWith("name-2019.03.01-"));

        p = new SnapshotLifecyclePolicy("id", "<name-{now/m{yyyy-MM-dd.HH:mm:ss}}>", "1 * * * * ?", "repo", Collections.emptyMap());
        assertThat(p.generateSnapshotName(context), startsWith("name-2019-03-15.21:09:00-"));
    }

    public void testValidation() {
        SnapshotLifecyclePolicy policy = new SnapshotLifecyclePolicy("a,b", "<my, snapshot-{now/M}>",
            "* * * * * L", "  ", Collections.emptyMap());

        ValidationException e = policy.validate();
        assertThat(e.validationErrors(),
            containsInAnyOrder("invalid policy id [a,b]: must not contain ','",
                "invalid snapshot name [<my, snapshot-{now/M}>]: must not contain contain" +
                    " the following characters [ , \", *, \\, <, |, ,, >, /, ?]",
                "invalid repository name [  ]: cannot be empty",
                "invalid schedule: invalid cron expression [* * * * * L]"));

        policy = new SnapshotLifecyclePolicy("_my_policy", "mySnap",
            " ", "repo", Collections.emptyMap());

        e = policy.validate();
        assertThat(e.validationErrors(),
            containsInAnyOrder("invalid policy id [_my_policy]: must not start with '_'",
                "invalid snapshot name [mySnap]: must be lowercase",
                "invalid schedule [ ]: must not be empty"));
    }

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
