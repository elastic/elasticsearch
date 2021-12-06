/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.transform.transforms;

import org.elasticsearch.Version;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.core.transform.transforms.TransformConfig;
import org.elasticsearch.xpack.core.transform.transforms.TransformConfigTests;
import org.elasticsearch.xpack.core.transform.transforms.latest.LatestConfigTests;
import org.elasticsearch.xpack.core.transform.transforms.pivot.PivotConfigTests;
import org.elasticsearch.xpack.transform.transforms.latest.Latest;
import org.elasticsearch.xpack.transform.transforms.pivot.Pivot;

import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.is;

public class FunctionFactoryTests extends ESTestCase {

    public void testCreatePivotFunction() {
        TransformConfig config = TransformConfigTests.randomTransformConfig(
            randomAlphaOfLengthBetween(1, 10),
            Version.CURRENT,
            PivotConfigTests.randomPivotConfig(),
            null
        );
        Function function = FunctionFactory.create(config);
        assertThat(function, is(instanceOf(Pivot.class)));
    }

    public void testCreateLatestFunction() {
        TransformConfig config = TransformConfigTests.randomTransformConfig(
            randomAlphaOfLengthBetween(1, 10),
            Version.CURRENT,
            null,
            LatestConfigTests.randomLatestConfig()
        );
        Function function = FunctionFactory.create(config);
        assertThat(function, is(instanceOf(Latest.class)));
    }
}
