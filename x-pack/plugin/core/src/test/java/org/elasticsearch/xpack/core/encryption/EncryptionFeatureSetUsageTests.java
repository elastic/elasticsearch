/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.core.encryption;

import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.test.AbstractWireSerializingTestCase;

public class EncryptionFeatureSetUsageTests extends AbstractWireSerializingTestCase<EncryptionFeatureSetUsage> {

    @Override
    protected Writeable.Reader<EncryptionFeatureSetUsage> instanceReader() {
        return EncryptionFeatureSetUsage::new;
    }

    @Override
    protected EncryptionFeatureSetUsage createTestInstance() {
        return new EncryptionFeatureSetUsage(randomBoolean(), randomBoolean());
    }

    @Override
    protected EncryptionFeatureSetUsage mutateInstance(EncryptionFeatureSetUsage instance) {
        return randomBoolean()
            ? new EncryptionFeatureSetUsage(instance.enabled() == false, instance.hasEncryptedData())
            : new EncryptionFeatureSetUsage(instance.enabled(), instance.hasEncryptedData() == false);
    }
}
