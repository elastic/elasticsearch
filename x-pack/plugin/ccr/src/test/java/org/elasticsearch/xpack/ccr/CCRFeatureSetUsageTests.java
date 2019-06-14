/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.ccr;

import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.test.AbstractWireSerializingTestCase;

public class CCRFeatureSetUsageTests extends AbstractWireSerializingTestCase<CCRFeatureSet.Usage> {

    @Override
    protected CCRFeatureSet.Usage createTestInstance() {
        return new CCRFeatureSet.Usage(randomBoolean(), randomBoolean(), randomIntBetween(0, Integer.MAX_VALUE),
            randomIntBetween(0, Integer.MAX_VALUE), randomNonNegativeLong());
    }

    @Override
    protected Writeable.Reader<CCRFeatureSet.Usage> instanceReader() {
        return CCRFeatureSet.Usage::new;
    }
}
