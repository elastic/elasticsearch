/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.core.ml.action;

import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.test.AbstractWireSerializingTestCase;
import org.elasticsearch.xpack.core.common.notifications.Level;

import java.io.IOException;

public class AuditMlNotificationActionRequestTests extends AbstractWireSerializingTestCase<AuditMlNotificationAction.Request> {
    @Override
    protected Writeable.Reader<AuditMlNotificationAction.Request> instanceReader() {
        return AuditMlNotificationAction.Request::new;
    }

    @Override
    protected AuditMlNotificationAction.Request createTestInstance() {
        return new AuditMlNotificationAction.Request(
            randomFrom(AuditMlNotificationAction.AuditType.values()),
            randomAlphaOfLength(3),
            randomAlphaOfLength(3),
            randomFrom(Level.values())
        );
    }

    @Override
    protected AuditMlNotificationAction.Request mutateInstance(AuditMlNotificationAction.Request instance) throws IOException {
        int field = randomIntBetween(0, 3);
        return switch (field) {
            case 0 -> {
                int nextEnum = (instance.getAuditType().ordinal() + 1) % AuditMlNotificationAction.AuditType.values().length;
                yield new AuditMlNotificationAction.Request(
                    AuditMlNotificationAction.AuditType.values()[nextEnum],
                    instance.getId(),
                    instance.getMessage(),
                    instance.getLevel()
                );
            }
            case 1 -> new AuditMlNotificationAction.Request(
                instance.getAuditType(),
                instance.getId() + "foo",
                instance.getMessage(),
                instance.getLevel()
            );
            case 2 -> new AuditMlNotificationAction.Request(
                instance.getAuditType(),
                instance.getId(),
                instance.getMessage() + "bar",
                instance.getLevel()
            );
            case 3 -> {
                int nextEnum = (instance.getLevel().ordinal() + 1) % Level.values().length;
                yield new AuditMlNotificationAction.Request(
                    instance.getAuditType(),
                    instance.getId(),
                    instance.getMessage(),
                    Level.values()[nextEnum]
                );
            }
            default -> {
                throw new AssertionError("unexpected case");
            }
        };
    }
}
