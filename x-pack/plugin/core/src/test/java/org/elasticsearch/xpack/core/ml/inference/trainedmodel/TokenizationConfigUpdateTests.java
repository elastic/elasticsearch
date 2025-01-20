/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.core.ml.inference.trainedmodel;

import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.test.AbstractWireSerializingTestCase;

import java.io.IOException;

public class TokenizationConfigUpdateTests extends AbstractWireSerializingTestCase<TokenizationConfigUpdate> {

    public static TokenizationConfigUpdate randomUpdate() {
        Integer maxSequenceLength = randomBoolean() ? null : randomIntBetween(32, 64);
        int span = randomIntBetween(8, 16);
        return new TokenizationConfigUpdate(maxSequenceLength, span);
    }

    @Override
    protected Writeable.Reader<TokenizationConfigUpdate> instanceReader() {
        return TokenizationConfigUpdate::new;
    }

    @Override
    protected TokenizationConfigUpdate createTestInstance() {
        return randomUpdate();
    }

    @Override
    protected TokenizationConfigUpdate mutateInstance(TokenizationConfigUpdate instance) throws IOException {
        return null;
    }
}
