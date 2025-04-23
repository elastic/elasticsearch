/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.mapper;

import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentType;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.math.BigInteger;
import java.nio.charset.StandardCharsets;

public class NumberTypeOutOfRangeSpec {

    final NumberFieldMapper.NumberType type;
    final Object value;
    final String message;

    public static NumberTypeOutOfRangeSpec of(NumberFieldMapper.NumberType t, Object v, String m) {
        return new NumberTypeOutOfRangeSpec(t, v, m);
    }

    NumberTypeOutOfRangeSpec(NumberFieldMapper.NumberType t, Object v, String m) {
        type = t;
        value = v;
        message = m;
    }

    public void write(XContentBuilder b) throws IOException {
        if (value instanceof BigInteger) {
            b.rawField("field", new ByteArrayInputStream(value.toString().getBytes(StandardCharsets.UTF_8)), XContentType.JSON);
        } else {
            b.field("field", value);
        }
    }
}
