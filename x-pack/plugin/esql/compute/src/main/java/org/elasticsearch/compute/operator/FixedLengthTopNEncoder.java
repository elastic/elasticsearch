/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.operator;

import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.BytesRefBuilder;

public class FixedLengthTopNEncoder implements TopNEncoder {

    @Override
    public void encodeBytesRef(BytesRef value, BytesRefBuilder bytesRefBuilder) {
        bytesRefBuilder.append(value);
    }

    @Override
    public String toString() {
        return "FixedLengthTopNEncoder";
    }
}
