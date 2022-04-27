/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.index.mapper;

import org.elasticsearch.xcontent.XContentBuilder;

import java.io.IOException;

public class IntegerRangeFieldMapperTests extends RangeFieldMapperTests {
    @Override
    protected XContentBuilder rangeSource(XContentBuilder in) throws IOException {
        return rangeSource(in, "1", "3");
    }

    @Override
    protected String storedValue() {
        return "1 : 3";
    }

    @Override
    protected Object rangeValue() {
        return 2;
    }

    @Override
    protected void minimalMapping(XContentBuilder b) throws IOException {
        b.field("type", "integer_range");
    }
}
