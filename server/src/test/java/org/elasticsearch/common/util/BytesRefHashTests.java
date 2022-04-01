/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.common.util;

public class BytesRefHashTests extends AbstractBytesRefHashTests {

    @Override
    protected AbstractBytesRefHash createTestInstance() {
        final float maxLoadFactor = 0.6f + randomFloat() * 0.39f;
        return new BytesRefHash(randomIntBetween(0, 100), maxLoadFactor, mockBigArrays());
    }

}
