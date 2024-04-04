/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.gradle.internal.docs;

import org.elasticsearch.gradle.fixtures.AbstractGradleInternalPluginFuncTest;
import org.elasticsearch.gradle.internal.conventions.precommit.PrecommitPlugin;
import org.elasticsearch.gradle.internal.precommit.CheckstylePrecommitPlugin;
import org.gradle.api.Plugin;

class Docs2PluginFuncTest extends AbstractGradleInternalPluginFuncTest {
    Class<? extends PrecommitPlugin> pluginClassUnderTest = Docs2Plugin.class
}
