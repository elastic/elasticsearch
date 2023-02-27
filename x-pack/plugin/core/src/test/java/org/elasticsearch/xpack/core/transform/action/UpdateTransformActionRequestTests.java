/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.core.transform.action;

import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.xpack.core.transform.action.UpdateTransformAction.Request;
import org.elasticsearch.xpack.core.transform.transforms.TransformConfigTests;
import org.elasticsearch.xpack.core.transform.transforms.TransformConfigUpdate;

import static org.elasticsearch.xpack.core.transform.transforms.TransformConfigUpdateTests.randomTransformConfigUpdate;

public class UpdateTransformActionRequestTests extends AbstractWireSerializingTransformTestCase<Request> {

    @Override
    protected Writeable.Reader<Request> instanceReader() {
        return Request::new;
    }

    @Override
    protected Request createTestInstance() {
        Request request = new Request(
            randomTransformConfigUpdate(),
            randomAlphaOfLength(10),
            randomBoolean(),
            TimeValue.parseTimeValue(randomTimeValue(), "timeout")
        );
        if (randomBoolean()) {
            request.setConfig(TransformConfigTests.randomTransformConfig());
        }
        return request;
    }

    @Override
    protected Request mutateInstance(Request instance) {
        String id = instance.getId();
        TransformConfigUpdate update = instance.getUpdate();
        boolean deferValidation = instance.isDeferValidation();
        TimeValue timeout = instance.getTimeout();

        switch (between(0, 3)) {
            case 0 -> id += randomAlphaOfLengthBetween(1, 5);
            case 1 -> {
                String description = update.getDescription() == null ? "" : update.getDescription();
                description += randomAlphaOfLengthBetween(1, 5);
                // fix corner case that description gets too long
                if (description.length() > 1000) {
                    description = description.substring(description.length() - 1000, description.length());
                }
                update = new TransformConfigUpdate(
                    update.getSource(),
                    update.getDestination(),
                    update.getFrequency(),
                    update.getSyncConfig(),
                    description,
                    update.getSettings(),
                    update.getMetadata(),
                    update.getRetentionPolicyConfig()
                );
            }
            case 2 -> deferValidation ^= true;
            case 3 -> timeout = new TimeValue(timeout.duration() + randomLongBetween(1, 5), timeout.timeUnit());
            default -> throw new AssertionError("Illegal randomization branch");
        }

        return new Request(update, id, deferValidation, timeout);
    }

}
