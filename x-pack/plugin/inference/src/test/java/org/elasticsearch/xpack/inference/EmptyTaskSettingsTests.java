/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference;

import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.inference.EmptyTaskSettings;
import org.elasticsearch.test.AbstractWireSerializingTestCase;

public class EmptyTaskSettingsTests extends AbstractWireSerializingTestCase<EmptyTaskSettings> {

    public static EmptyTaskSettings createRandom() {
        return EmptyTaskSettings.INSTANCE; // no options to randomise
    }

    @Override
    protected Writeable.Reader<EmptyTaskSettings> instanceReader() {
        return EmptyTaskSettings::new;
    }

    @Override
    protected EmptyTaskSettings createTestInstance() {
        return createRandom();
    }

    @Override
    protected EmptyTaskSettings mutateInstance(EmptyTaskSettings instance) {
        return null;
    }
}
