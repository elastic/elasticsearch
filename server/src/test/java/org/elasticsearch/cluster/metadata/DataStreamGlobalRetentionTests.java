/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.cluster.metadata;

import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.test.AbstractWireSerializingTestCase;

public class DataStreamGlobalRetentionTests extends AbstractWireSerializingTestCase<DataStreamGlobalRetention> {

    @Override
    protected Writeable.Reader<DataStreamGlobalRetention> instanceReader() {
        return DataStreamGlobalRetention::read;
    }

    @Override
    protected DataStreamGlobalRetention createTestInstance() {
        return randomGlobalRetention();
    }

    @Override
    protected DataStreamGlobalRetention mutateInstance(DataStreamGlobalRetention instance) {
        var defaultRetention = instance.defaultRetention();
        var maxRetention = instance.maxRetention();
        switch (randomInt(1)) {
            case 0 -> {
                defaultRetention = randomValueOtherThan(
                    defaultRetention,
                    () -> randomBoolean() ? null : TimeValue.timeValueDays(randomIntBetween(1, 1000))
                );
            }
            case 1 -> {
                maxRetention = randomValueOtherThan(
                    maxRetention,
                    () -> randomBoolean() ? null : TimeValue.timeValueDays(randomIntBetween(1001, 2000))
                );
            }
        }
        return new DataStreamGlobalRetention(defaultRetention, maxRetention);
    }

    public static DataStreamGlobalRetention randomGlobalRetention() {
        boolean withDefault = randomBoolean();
        boolean withMax = randomBoolean();
        return new DataStreamGlobalRetention(
            withDefault == false ? null : TimeValue.timeValueDays(randomIntBetween(1, 1000)),
            withMax == false ? null : TimeValue.timeValueDays(randomIntBetween(1000, 2000))
        );
    }

    public void testValidation() {
        expectThrows(
            IllegalArgumentException.class,
            () -> new DataStreamGlobalRetention(
                TimeValue.timeValueDays(randomIntBetween(1001, 2000)),
                TimeValue.timeValueDays(randomIntBetween(1, 1000))
            )
        );
        expectThrows(IllegalArgumentException.class, () -> new DataStreamGlobalRetention(TimeValue.ZERO, null));
        expectThrows(IllegalArgumentException.class, () -> new DataStreamGlobalRetention(null, TimeValue.ZERO));
    }
}
