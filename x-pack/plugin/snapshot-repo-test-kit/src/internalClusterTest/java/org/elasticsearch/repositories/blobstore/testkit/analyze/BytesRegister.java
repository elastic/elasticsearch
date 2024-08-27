/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.repositories.blobstore.testkit.analyze;

import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.bytes.BytesReference;

import java.util.function.UnaryOperator;

class BytesRegister {

    private BytesReference bytesReference = BytesArray.EMPTY;

    synchronized BytesReference compareAndExchange(BytesReference expected, BytesReference updated) {
        if (bytesReference.equals(expected)) {
            bytesReference = updated;
            return expected;
        } else {
            return bytesReference;
        }
    }

    synchronized void updateAndGet(UnaryOperator<BytesReference> updater) {
        bytesReference = updater.apply(bytesReference);
    }

    synchronized BytesReference get() {
        return bytesReference;
    }
}
