/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.security;

import org.elasticsearch.analysis.common.CommonAnalysisPlugin;
import org.elasticsearch.index.reindex.ReindexPlugin;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.test.ESIntegTestCase;
import org.elasticsearch.test.InternalSettingsPlugin;
import org.elasticsearch.test.SecurityIntegTestCase;
import org.elasticsearch.transport.Netty4Plugin;
import org.elasticsearch.xpack.core.XPackPlugin;

import java.util.Arrays;
import java.util.Collection;

@ESIntegTestCase.ClusterScope(scope = ESIntegTestCase.Scope.TEST, numDataNodes = 0)
public class SecurityPluginNoDeprecationTests extends SecurityIntegTestCase {

    @Override
    public String getSecPluginName() { return Security.class.getName(); }

    @Override
    protected boolean enableWarningsCheck() {
        return true;
    }

    @Override
    public Collection<Class<? extends Plugin>> nodePlugins() {
        return Arrays.asList(XPackPlugin.class, Security.class, Netty4Plugin.class, ReindexPlugin.class, CommonAnalysisPlugin.class,
            InternalSettingsPlugin.class);
    }

    public void testNoDeprecationMessage() {
        ensureNoDeprecationOnSecurityPlugin();
        return;
    }
}
