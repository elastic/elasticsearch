/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.core.constantkeyword;

import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.test.AbstractWireSerializingTestCase;

import java.io.IOException;

public class ConstantKeywordFeatureSetUsageTests extends AbstractWireSerializingTestCase<ConstantKeywordFeatureSetUsage> {

    @Override
    protected ConstantKeywordFeatureSetUsage createTestInstance() {
        return new ConstantKeywordFeatureSetUsage(randomBoolean(), randomIntBetween(0, 1000));
    }

    @Override
    protected ConstantKeywordFeatureSetUsage mutateInstance(ConstantKeywordFeatureSetUsage instance) throws IOException {

        boolean available = instance.available();
        int fieldCount = instance.fieldCount();

        switch (between(0, 1)) {
            case 0:
                available = !available;
                break;
            case 1:
                fieldCount = randomValueOtherThan(instance.fieldCount(), () -> randomIntBetween(0, 1000));
                break;
        }

        return new ConstantKeywordFeatureSetUsage(available, fieldCount);
    }

    @Override
    protected Writeable.Reader<ConstantKeywordFeatureSetUsage> instanceReader() {
        return ConstantKeywordFeatureSetUsage::new;
    }

}
