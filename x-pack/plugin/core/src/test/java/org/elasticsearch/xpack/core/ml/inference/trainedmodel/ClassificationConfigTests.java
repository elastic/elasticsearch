/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.core.ml.inference.trainedmodel;

import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.test.AbstractWireSerializingTestCase;

public class ClassificationConfigTests extends AbstractWireSerializingTestCase<ClassificationConfig> {

    public static ClassificationConfig randomClassificationConfig() {
        return new ClassificationConfig(randomBoolean() ? null : randomIntBetween(-1, 10));
    }

    @Override
    protected ClassificationConfig createTestInstance() {
        return randomClassificationConfig();
    }

    @Override
    protected Writeable.Reader<ClassificationConfig> instanceReader() {
        return ClassificationConfig::new;
    }

}
