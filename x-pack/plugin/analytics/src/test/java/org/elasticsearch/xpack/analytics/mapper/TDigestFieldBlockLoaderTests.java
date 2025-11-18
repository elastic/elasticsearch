/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.analytics.mapper;

import org.elasticsearch.core.Types;
import org.elasticsearch.index.mapper.BlockLoaderTestCase;

import java.util.Map;

public class TDigestFieldBlockLoaderTests extends BlockLoaderTestCase {

    public TDigestFieldBlockLoaderTests(Params params) {
        super(TDigestFieldMapper.CONTENT_TYPE, params);
    }

    @Override
    protected Object expected(Map<String, Object> fieldMapping, Object value, TestContext testContext) {
        Map<String, Object> valueAsMap = Types.forciblyCast(value);
        return valueAsMap;
    }
}
