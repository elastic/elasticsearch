/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.test.eql;

import com.carrotsearch.randomizedtesting.annotations.ParametersFactory;

import java.util.HashSet;
import java.util.List;

import static org.elasticsearch.test.eql.DataLoader.TEST_EXTRA_INDEX;

public abstract class EqlExtraSpecTestCase extends BaseEqlSpecTestCase {

    @ParametersFactory(shuffle = false, argumentFormatting = PARAM_FORMATTING)
    public static List<Object[]> readTestSpecs() throws Exception {
        return asArray(EqlSpecLoader.load("/test_extra.toml", true, new HashSet<>()));
    }

    public EqlExtraSpecTestCase(String query, String name, long[] eventIds, boolean caseSensitive) {
        super(TEST_EXTRA_INDEX, query, name, eventIds, caseSensitive);
    }
}
