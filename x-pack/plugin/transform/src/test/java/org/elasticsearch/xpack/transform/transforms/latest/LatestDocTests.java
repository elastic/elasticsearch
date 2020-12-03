/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.transform.transforms.latest;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.core.transform.transforms.latest.LatestDocConfig;
import org.elasticsearch.xpack.core.transform.transforms.latest.LatestDocConfigTests;
import org.elasticsearch.xpack.transform.transforms.Function;

import static org.hamcrest.Matchers.is;

public class LatestDocTests extends ESTestCase {

    public void testValidateConfig() {
        LatestDocConfig latestConfig = LatestDocConfigTests.randomLatestConfig();
        Function latest = new LatestDoc(latestConfig);
        latest.validateConfig(
            ActionListener.wrap(
                isValid -> assertThat(isValid, is(true)),
                e -> fail(e.getMessage())));
    }
}
