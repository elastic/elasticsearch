/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.ccr;

import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.test.AbstractWireSerializingTestCase;

public class CCRFeatureSetUsageTests extends AbstractWireSerializingTestCase<CCRInfoTransportAction.Usage> {

    @Override
    protected CCRInfoTransportAction.Usage createTestInstance() {
        return new CCRInfoTransportAction.Usage(
            randomBoolean(),
            randomBoolean(),
            randomIntBetween(0, Integer.MAX_VALUE),
            randomIntBetween(0, Integer.MAX_VALUE),
            randomNonNegativeLong()
        );
    }

    @Override
    protected Writeable.Reader<CCRInfoTransportAction.Usage> instanceReader() {
        return CCRInfoTransportAction.Usage::new;
    }
}
