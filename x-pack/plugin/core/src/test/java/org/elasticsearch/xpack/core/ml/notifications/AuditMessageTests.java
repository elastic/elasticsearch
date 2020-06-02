/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.core.ml.notifications;

import org.elasticsearch.test.AbstractXContentTestCase;
import org.elasticsearch.xpack.core.common.notifications.AbstractAuditMessage;


import static org.hamcrest.Matchers.equalTo;

public abstract class AuditMessageTests<T extends AbstractAuditMessage> extends AbstractXContentTestCase<T> {

    public abstract String getJobType();

    public void testGetJobType() {
        AbstractAuditMessage message = createTestInstance();
        assertThat(message.getJobType(), equalTo(getJobType()));
    }

    @Override
    protected boolean supportsUnknownFields() {
        return true;
    }
}
