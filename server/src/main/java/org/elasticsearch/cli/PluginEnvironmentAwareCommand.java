/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.elasticsearch.cli;

import joptsimple.OptionSet;
import org.elasticsearch.common.settings.KeyStoreWrapper;
import org.elasticsearch.common.settings.SecureSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.NamedXContentRegistry;
import org.elasticsearch.env.Environment;
import org.elasticsearch.node.Node;
import org.elasticsearch.plugins.PluginsService;
import org.elasticsearch.plugins.SearchPlugin;
import org.elasticsearch.search.SearchModule;

import java.util.Collections;
import java.util.Map;

public abstract class PluginEnvironmentAwareCommand extends EnvironmentAwareCommand {

    public PluginEnvironmentAwareCommand(String description) {
        super(description);
    }

    @Override
    public final void execute(Terminal terminal, OptionSet options, Environment env) throws Exception {
        final PluginsService pluginsService = new PluginsService(env.settings(), env.configFile(), env.modulesFile(),
            env.pluginsFile(), Collections.emptyList());
        final Settings settings = pluginsService.updatedSettings();
        final SearchModule searchModule = new SearchModule(settings, pluginsService.filterPlugins(SearchPlugin.class));
        final NamedXContentRegistry xContentRegistry = Node.createNamedXContentRegistry(searchModule, pluginsService);
        execute(terminal, options, env, xContentRegistry);
    }

    @Override
    protected Environment createEnv(final Map<String, String> settings) throws UserException {
        final Environment initialEnv = super.createEnv(settings);
        final SecureSettings secureSettings;
        try {
            final KeyStoreWrapper keystore = KeyStoreWrapper.load(initialEnv.configFile());
            if (keystore != null) {
                keystore.decrypt(new char[0] /* TODO: read password from stdin */);
            }
            secureSettings = keystore;
        } catch (Exception e) {
            throw new UserException(ExitCodes.CONFIG, "can't load keystore", e);
        }

        if (secureSettings != null) {
            return createEnv(Settings.builder().setSecureSettings(secureSettings).build(), settings);
        } else {
            return initialEnv;
        }
    }

    /** Execute the command with the initialized {@link Environment} and {@link NamedXContentRegistry}. */
    protected abstract void execute(Terminal terminal, OptionSet options, Environment env, NamedXContentRegistry xContentRegistry)
        throws Exception;
}
