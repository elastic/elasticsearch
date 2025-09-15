/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.mapper;

public class LongOffsetDocValuesLoaderTests extends OffsetDocValuesLoaderTestCase {

    public void testOffsetArray() throws Exception {
        verifyOffsets("{\"field\":[26,24,25,3,2,1]}");
        verifyOffsets("{\"field\":[26,null,25,3,null,1]}");
        verifyOffsets("{\"field\":[5,5,6,-3,-9,-9,5,2,5,6,-3,-9]}");
    }

    public void testOffsetNestedArray() throws Exception {
        verifyOffsets("{\"field\":[\"26\",[\"24\"],[\"3\"],null,\"1\"]}", "{\"field\":[26,24,3,null,1]}");
        verifyOffsets("{\"field\":[\"26\",[\"24\", [\"11\"]],[\"3\", [\"12\"]],null,\"1\"]}", "{\"field\":[26,24,11,3,12,null,1]}");
    }

    @Override
    protected String getFieldTypeName() {
        return "long";
    }

    @Override
    protected Object randomValue() {
        return randomLong();
    }
}
