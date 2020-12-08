/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.transform.transforms.latest;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.core.transform.transforms.latest.LatestConfig;
import org.elasticsearch.xpack.core.transform.transforms.latest.LatestConfigTests;
import org.elasticsearch.xpack.transform.transforms.Function;

import static org.hamcrest.Matchers.is;

public class LatestTests extends ESTestCase {

    public void testValidateConfig() {
        LatestConfig latestConfig = LatestConfigTests.randomLatestConfig();
        Function latest = new Latest(latestConfig);
        latest.validateConfig(
            ActionListener.wrap(
                isValid -> assertThat(isValid, is(true)),
                e -> fail(e.getMessage())));
    }
}
