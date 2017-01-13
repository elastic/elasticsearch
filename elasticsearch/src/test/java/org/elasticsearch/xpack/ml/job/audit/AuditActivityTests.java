/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.ml.job.audit;

import org.elasticsearch.common.io.stream.Writeable.Reader;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.xpack.ml.support.AbstractSerializingTestCase;
import org.elasticsearch.xpack.ml.utils.time.TimeUtils;
import org.junit.Before;

import java.util.Date;

public class AuditActivityTests extends AbstractSerializingTestCase<AuditActivity> {
    private long startMillis;

    @Before
    public void setStartTime() {
        startMillis = System.currentTimeMillis();
    }

    public void testDefaultConstructor() {
        AuditActivity activity = new AuditActivity();
        assertEquals(0, activity.getTotalJobs());
        assertEquals(0, activity.getTotalDetectors());
        assertEquals(0, activity.getRunningJobs());
        assertEquals(0, activity.getRunningDetectors());
        assertNull(activity.getTimestamp());
    }

    public void testNewActivity() {
        AuditActivity activity = AuditActivity.newActivity(10, 100, 5, 50);
        assertEquals(10, activity.getTotalJobs());
        assertEquals(100, activity.getTotalDetectors());
        assertEquals(5, activity.getRunningJobs());
        assertEquals(50, activity.getRunningDetectors());
        assertDateBetweenStartAndNow(activity.getTimestamp());
    }

    private void assertDateBetweenStartAndNow(Date timestamp) {
        long timestampMillis = timestamp.getTime();
        assertTrue(timestampMillis >= startMillis);
        assertTrue(timestampMillis <= System.currentTimeMillis());
    }

    @Override
    protected AuditActivity parseInstance(XContentParser parser) {
        return AuditActivity.PARSER.apply(parser, null);
    }

    @Override
    protected AuditActivity createTestInstance() {
        AuditActivity message = new AuditActivity();
        if (randomBoolean()) {
            message.setRunningJobs(randomInt());
        }
        if (randomBoolean()) {
            message.setRunningDetectors(randomInt());
        }
        if (randomBoolean()) {
            message.setTotalJobs(randomInt());
        }
        if (randomBoolean()) {
            message.setTotalDetectors(randomInt());
        }
        if (randomBoolean()) {
            message.setTimestamp(new Date(TimeUtils.dateStringToEpoch(randomTimeValue())));
        }
        return message;
    }

    @Override
    protected Reader<AuditActivity> instanceReader() {
        return AuditActivity::new;
    }
}
