/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.test;

import org.elasticsearch.Version;
import org.elasticsearch.bootstrap.BootstrapContext;
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.env.Environment;

import java.nio.file.Path;

public abstract class AbstractBootstrapCheckTestCase extends ESTestCase {
    protected final BootstrapContext emptyContext;

    public AbstractBootstrapCheckTestCase() {
        emptyContext = createTestContext(Settings.EMPTY, Metadata.EMPTY_METADATA);
    }

    protected BootstrapContext createTestContext(Settings settings, Metadata metadata) {
        Path homePath = createTempDir();
        Environment environment = new Environment(
            settings(Version.CURRENT).put(settings).put(Environment.PATH_HOME_SETTING.getKey(), homePath.toString()).build(),
            null
        );
        return new BootstrapContext(environment, metadata);
    }
}
