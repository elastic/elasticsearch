/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.rollup;

import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.xpack.core.LocalStateCompositeXPackPlugin;

import java.nio.file.Path;

public class LocalStateRollup extends LocalStateCompositeXPackPlugin {

    public LocalStateRollup(final Settings settings, final Path configPath) throws Exception {
        super(settings, configPath);
        plugins.add(new Rollup(settings));
    }
}

