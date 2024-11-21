/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.plugins;

import org.elasticsearch.test.ESTestCase;

import static org.hamcrest.Matchers.equalTo;

public class PluginsLoaderTests extends ESTestCase {

    public void testToModuleName() {
        assertThat(PluginsLoader.toModuleName("module.name"), equalTo("module.name"));
        assertThat(PluginsLoader.toModuleName("module-name"), equalTo("module.name"));
        assertThat(PluginsLoader.toModuleName("module-name1"), equalTo("module.name1"));
        assertThat(PluginsLoader.toModuleName("1module-name"), equalTo("module.name"));
        assertThat(PluginsLoader.toModuleName("module-name!"), equalTo("module.name"));
        assertThat(PluginsLoader.toModuleName("module!@#name!"), equalTo("module.name"));
        assertThat(PluginsLoader.toModuleName("!module-name!"), equalTo("module.name"));
        assertThat(PluginsLoader.toModuleName("module_name"), equalTo("module_name"));
        assertThat(PluginsLoader.toModuleName("-module-name-"), equalTo("module.name"));
        assertThat(PluginsLoader.toModuleName("_module_name"), equalTo("_module_name"));
        assertThat(PluginsLoader.toModuleName("_"), equalTo("_"));
    }
}
