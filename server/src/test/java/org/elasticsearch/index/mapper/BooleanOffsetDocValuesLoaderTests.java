/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.mapper;

public class BooleanOffsetDocValuesLoaderTests extends OffsetDocValuesLoaderTestCase {

    public void testOffsetArray() throws Exception {
        verifyOffsets("{\"field\":[true,false,true,true,false,true]}");
        verifyOffsets("{\"field\":[true,null,false,false,null,null,true,false]}");
        verifyOffsets("{\"field\":[true,true,true,true]}");
    }

    public void testOffsetNestedArray() throws Exception {
        verifyOffsets("{\"field\":[[\"true\",[false,[true]]],[\"true\",false,true]]}", "{\"field\":[true,false,true,true,false,true]}");
        verifyOffsets(
            "{\"field\":[true,[null,[[false,false],[null,null]],[true,false]]]}",
            "{\"field\":[true,null,false,false,null,null,true,false]}"
        );
    }

    @Override
    protected String getFieldTypeName() {
        return "boolean";
    }

    @Override
    protected Object randomValue() {
        return randomBoolean();
    }
}
