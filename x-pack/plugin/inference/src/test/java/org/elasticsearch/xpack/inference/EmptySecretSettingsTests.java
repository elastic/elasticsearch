/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference;

import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.core.Tuple;
import org.elasticsearch.inference.EmptySecretSettings;
import org.elasticsearch.test.AbstractWireSerializingTestCase;

public class EmptySecretSettingsTests extends AbstractWireSerializingTestCase<EmptySecretSettings> {

    public static EmptySecretSettings createRandom() {
        return EmptySecretSettings.INSTANCE; // no options to randomise
    }

    @Override
    protected Writeable.Reader<EmptySecretSettings> instanceReader() {
        return EmptySecretSettings::new;
    }

    @Override
    protected EmptySecretSettings createTestInstance() {
        return createRandom();
    }

    @Override
    protected EmptySecretSettings mutateInstance(EmptySecretSettings instance) {
        // All instances are the same and have no fields, nothing to mutate
        return null;
    }

    public void testNewSecretSettings() {

        EmptySecretSettings newSecretSettings = (EmptySecretSettings) EmptySecretSettings.INSTANCE.newSecretSettings(
            randomMap(0, 3, () -> new Tuple<>(randomAlphaOfLengthBetween(1, 10), randomAlphaOfLengthBetween(1, 10)))
        );

        assertSame(EmptySecretSettings.INSTANCE, newSecretSettings);
    }
}
