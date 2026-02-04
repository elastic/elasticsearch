/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.mapper;

public class KeywordOffsetDocValuesLoaderTests extends OffsetDocValuesLoaderTestCase {

    public void testOffsetArray() throws Exception {
        verifyOffsets("{\"field\":[\"z\",\"x\",\"y\",\"c\",\"b\",\"a\"]}");
        verifyOffsets("{\"field\":[\"z\",null,\"y\",\"c\",null,\"a\"]}");
    }

    public void testOffsetNestedArray() throws Exception {
        verifyOffsets("{\"field\":[\"z\",[\"y\"],[\"c\"],null,\"a\"]}", "{\"field\":[\"z\",\"y\",\"c\",null,\"a\"]}");
        verifyOffsets(
            "{\"field\":[\"z\",[\"y\", [\"k\"]],[\"c\", [\"l\"]],null,\"a\"]}",
            "{\"field\":[\"z\",\"y\",\"k\",\"c\",\"l\",null,\"a\"]}"
        );
    }

    @Override
    protected String getFieldTypeName() {
        return "keyword";
    }

    @Override
    protected Object randomValue() {
        return randomAlphanumericOfLength(2);
    }
}
