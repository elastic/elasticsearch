/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.deprecation;

import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.test.ESTestCase;

import static org.hamcrest.Matchers.is;

public class TransformDeprecationCheckerTests extends ESTestCase {

    public void testEnabled() {
        TransformDeprecationChecker transformDeprecationChecker = new TransformDeprecationChecker();
        assertThat(transformDeprecationChecker.enabled(Settings.EMPTY), is(true));
    }
}
