/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.core.ilm;

import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.common.Strings;

import static org.elasticsearch.xpack.core.ilm.GenerateSnapshotNameStep.generateSnapshotName;
import static org.elasticsearch.xpack.core.ilm.GenerateSnapshotNameStep.validateGeneratedSnapshotName;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.nullValue;
import static org.hamcrest.Matchers.startsWith;

public class GenerateSnapshotNameStepTests extends AbstractStepTestCase<GenerateSnapshotNameStep> {

    @Override
    protected GenerateSnapshotNameStep createRandomInstance() {
        return new GenerateSnapshotNameStep(randomStepKey(), randomStepKey());
    }

    @Override
    protected GenerateSnapshotNameStep mutateInstance(GenerateSnapshotNameStep instance) {
        Step.StepKey key = instance.getKey();
        Step.StepKey nextKey = instance.getNextStepKey();

        switch (between(0, 1)) {
            case 0:
                key = new Step.StepKey(key.getPhase(), key.getAction(), key.getName() + randomAlphaOfLength(5));
                break;
            case 1:
                nextKey = new Step.StepKey(key.getPhase(), key.getAction(), key.getName() + randomAlphaOfLength(5));
                break;
            default:
                throw new AssertionError("Illegal randomisation branch");
        }
        return new GenerateSnapshotNameStep(key, nextKey);
    }

    @Override
    protected GenerateSnapshotNameStep copyInstance(GenerateSnapshotNameStep instance) {
        return new GenerateSnapshotNameStep(instance.getKey(), instance.getNextStepKey());
    }

    public void testNameGeneration() {
        long time = 1552684146542L; // Fri Mar 15 2019 21:09:06 UTC
        assertThat(generateSnapshotName("name"), startsWith("name-"));
        assertThat(generateSnapshotName("name").length(), greaterThan("name-".length()));

        GenerateSnapshotNameStep.ResolverContext resolverContext = new GenerateSnapshotNameStep.ResolverContext(time);
        assertThat(generateSnapshotName("<name-{now}>", resolverContext), startsWith("name-2019.03.15-"));
        assertThat(generateSnapshotName("<name-{now}>", resolverContext).length(), greaterThan("name-2019.03.15-".length()));

        assertThat(generateSnapshotName("<name-{now/M}>", resolverContext), startsWith("name-2019.03.01-"));

        assertThat(generateSnapshotName("<name-{now/m{yyyy-MM-dd.HH:mm:ss}}>", resolverContext), startsWith("name-2019-03-15.21:09:00-"));
    }

    public void testNameValidation() {
        assertThat(validateGeneratedSnapshotName("name-", generateSnapshotName("name-")), nullValue());
        assertThat(validateGeneratedSnapshotName("<name-{now}>", generateSnapshotName("<name-{now}>")), nullValue());

        {
            ActionRequestValidationException validationException = validateGeneratedSnapshotName("", generateSnapshotName(""));
            assertThat(validationException, notNullValue());
            assertThat(validationException.validationErrors(), containsInAnyOrder("invalid snapshot name []: cannot be empty"));
        }
        {
            ActionRequestValidationException validationException = validateGeneratedSnapshotName("#start", generateSnapshotName("#start"));
            assertThat(validationException, notNullValue());
            assertThat(validationException.validationErrors(), containsInAnyOrder("invalid snapshot name [#start]: must not contain '#'"));
        }
        {
            ActionRequestValidationException validationException = validateGeneratedSnapshotName("_start", generateSnapshotName("_start"));
            assertThat(validationException, notNullValue());
            assertThat(validationException.validationErrors(), containsInAnyOrder("invalid snapshot name [_start]: must not start with " +
                "'_'"));
        }
        {
            ActionRequestValidationException validationException = validateGeneratedSnapshotName("aBcD", generateSnapshotName("aBcD"));
            assertThat(validationException, notNullValue());
            assertThat(validationException.validationErrors(), containsInAnyOrder("invalid snapshot name [aBcD]: must be lowercase"));
        }
        {
            ActionRequestValidationException validationException = validateGeneratedSnapshotName("na>me", generateSnapshotName("na>me"));
            assertThat(validationException, notNullValue());
            assertThat(validationException.validationErrors(), containsInAnyOrder("invalid snapshot name [na>me]: must not contain " +
                "contain the following characters " + Strings.INVALID_FILENAME_CHARS));
        }
    }
}
