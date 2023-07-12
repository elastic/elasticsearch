/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.application;

import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.xpack.core.LocalStateCompositeXPackPlugin;

import java.nio.file.Path;

public class LocalStateEnterpriseSearch extends LocalStateCompositeXPackPlugin {
    public LocalStateEnterpriseSearch(Settings settings, Path configPath) {
        super(settings, configPath);

        plugins.add(new EnterpriseSearch(settings));
    }

}
