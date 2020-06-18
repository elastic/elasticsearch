/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.async;

import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.test.ESTestCase;
import org.hamcrest.Matchers;

public class AsyncResultsIndexPluginTests extends ESTestCase {

    public void testDummy() {
        // This is a dummy test case to satisfy the conventions
        AsyncResultsIndexPlugin plugin = new AsyncResultsIndexPlugin(Settings.EMPTY);
        assertThat(plugin.getSystemIndexDescriptors(Settings.EMPTY), Matchers.hasSize(1));
    }
}
