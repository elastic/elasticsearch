/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.common.settings;

import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.plugins.PluginsService;

import java.util.List;
import java.util.Set;

public interface PublicSettingsFactory {

    PublicSettings create(ThreadContext threadContext, Set<Setting<?>> serverlessPublicSettings);

    static PublicSettingsFactory load(PluginsService pluginsService) {
        // in a draft this is in a security plugin. In serverless we could keep this in a lib with compileOnly dependnecy on server
        // and use ServiceLoader
        List<? extends PublicSettingsFactory> factories = pluginsService.loadServiceProviders(PublicSettingsFactory.class);
        if (factories.size() > 1) {
            throw new IllegalStateException("More than one publicSettings factory found");
        } else if (factories.size() == 0) {
            return new DefaultPublicSettingsFactory();
        }
        return factories.get(0);
    }

    class DefaultPublicSettingsFactory implements PublicSettingsFactory {
        @Override
        public PublicSettings create(ThreadContext threadContext, Set<Setting<?>> serverlessPublicSettings) {
            return new PublicSettings.DefaultPublicSettings();
        }
    }
}
