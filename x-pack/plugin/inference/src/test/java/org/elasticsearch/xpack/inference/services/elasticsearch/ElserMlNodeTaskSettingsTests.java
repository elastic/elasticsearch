/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.elasticsearch;

import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.test.AbstractWireSerializingTestCase;

public class ElserMlNodeTaskSettingsTests extends AbstractWireSerializingTestCase<ElserMlNodeTaskSettings> {

    public static ElserMlNodeTaskSettings createRandom() {
        return ElserMlNodeTaskSettings.DEFAULT; // no options to randomise
    }

    @Override
    protected Writeable.Reader<ElserMlNodeTaskSettings> instanceReader() {
        return ElserMlNodeTaskSettings::new;
    }

    @Override
    protected ElserMlNodeTaskSettings createTestInstance() {
        return createRandom();
    }

    @Override
    protected ElserMlNodeTaskSettings mutateInstance(ElserMlNodeTaskSettings instance) {
        return null;
    }
}
