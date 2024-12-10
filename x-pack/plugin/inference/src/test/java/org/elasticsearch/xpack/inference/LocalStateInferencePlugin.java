/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference;

import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.license.XPackLicenseState;
import org.elasticsearch.plugins.SearchPlugin;
import org.elasticsearch.xpack.core.LocalStateCompositeXPackPlugin;
import org.elasticsearch.xpack.core.ssl.SSLService;

import java.nio.file.Path;
import java.util.List;

import static java.util.stream.Collectors.toList;

public class LocalStateInferencePlugin extends LocalStateCompositeXPackPlugin {
    private final InferencePlugin inferencePlugin;

    public LocalStateInferencePlugin(final Settings settings, final Path configPath) throws Exception {
        super(settings, configPath);
        LocalStateInferencePlugin thisVar = this;
        this.inferencePlugin = new InferencePlugin(settings) {
            @Override
            protected SSLService getSslService() {
                return thisVar.getSslService();
            }

            @Override
            protected XPackLicenseState getLicenseState() {
                return thisVar.getLicenseState();
            }
        };
        plugins.add(inferencePlugin);
    }

    @Override
    public List<RetrieverSpec<?>> getRetrievers() {
        return this.filterPlugins(SearchPlugin.class).stream().flatMap(p -> p.getRetrievers().stream()).collect(toList());
    }
}
