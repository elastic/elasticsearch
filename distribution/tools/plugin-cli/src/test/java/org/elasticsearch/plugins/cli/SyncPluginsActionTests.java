/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
package org.elasticsearch.plugins.cli;

import org.apache.lucene.tests.util.LuceneTestCase;
import org.elasticsearch.Version;
import org.elasticsearch.cli.MockTerminal;
import org.elasticsearch.cli.UserException;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.env.Environment;
import org.elasticsearch.env.TestEnvironment;
import org.elasticsearch.plugins.PluginTestUtil;
import org.elasticsearch.plugins.cli.SyncPluginsAction.PluginChanges;
import org.elasticsearch.test.ESTestCase;
import org.hamcrest.Matchers;
import org.junit.Before;
import org.mockito.InOrder;
import org.mockito.Mockito;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Optional;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.is;
import static org.mockito.ArgumentMatchers.anyList;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;

@LuceneTestCase.SuppressFileSystems("*")
public class SyncPluginsActionTests extends ESTestCase {
    private Environment env;
    private SyncPluginsAction action;
    private PluginsConfig config;
    private MockTerminal terminal;

    @Override
    @Before
    public void setUp() throws Exception {
        super.setUp();
        Path home = createTempDir();
        Settings settings = Settings.builder().put("path.home", home).build();
        env = TestEnvironment.newEnvironment(settings);
        Files.createDirectories(env.binFile());
        Files.createFile(env.binFile().resolve("elasticsearch"));
        Files.createDirectories(env.configFile());
        Files.createDirectories(env.pluginsFile());

        terminal = MockTerminal.create();
        action = new SyncPluginsAction(terminal, env);
        config = new PluginsConfig();
    }

    /**
     * Check that when we ensure a plugins config file doesn't exist, and it really doesn't exist,
     * then no exception is thrown.
     */
    public void test_ensureNoConfigFile_withoutConfig_doesNothing() throws Exception {
        SyncPluginsAction.ensureNoConfigFile(env);
    }

    /**
     * Check that when we ensure a plugins config file doesn't exist, but a file does exist,
     * then an exception is thrown.
     */
    public void test_ensureNoConfigFile_withConfig_throwsException() throws Exception {
        Files.createFile(env.configFile().resolve("elasticsearch-plugins.yml"));
        final UserException e = expectThrows(UserException.class, () -> SyncPluginsAction.ensureNoConfigFile(env));

        assertThat(e.getMessage(), Matchers.matchesPattern("^Plugins config \\[.*] exists.*$"));
    }

    /**
     * Check that when there are no plugins to install, and no plugins already installed, then we
     * calculate that no changes are required.
     */
    public void test_getPluginChanges_withNoChanges_returnsNoChanges() throws PluginSyncException {
        final SyncPluginsAction.PluginChanges pluginChanges = action.getPluginChanges(config, Optional.empty());

        assertThat(pluginChanges.isEmpty(), is(true));
    }

    /**
     * Check that when there are no plugins in the config file, and a plugin is already installed, then we
     * calculate that the plugin needs to be removed.
     */
    public void test_getPluginChanges_withExtraPluginOnDisk_returnsPluginToRemove() throws Exception {
        createPlugin("my-plugin");

        final PluginChanges pluginChanges = action.getPluginChanges(config, Optional.empty());

        assertThat(pluginChanges.isEmpty(), is(false));
        assertThat(pluginChanges.install, empty());
        assertThat(pluginChanges.remove, hasSize(1));
        assertThat(pluginChanges.upgrade, empty());
        assertThat(pluginChanges.remove.get(0).getId(), equalTo("my-plugin"));
    }

    /**
     * Check that when there is a plugin in the config file, and no plugins already installed, then we
     * calculate that the plugin needs to be installed.
     */
    public void test_getPluginChanges_withPluginToInstall_returnsPluginToInstall() throws Exception {
        config.setPlugins(List.of(new InstallablePlugin("my-plugin")));

        final PluginChanges pluginChanges = action.getPluginChanges(config, Optional.empty());

        assertThat(pluginChanges.isEmpty(), is(false));
        assertThat(pluginChanges.install, hasSize(1));
        assertThat(pluginChanges.remove, empty());
        assertThat(pluginChanges.upgrade, empty());
        assertThat(pluginChanges.install.get(0).getId(), equalTo("my-plugin"));
    }

    /**
     * Check that when there is an unofficial plugin in the config file, and that plugin is already installed
     * but needs to be upgraded due to the Elasticsearch version, then we calculate that no changes are required,
     * since we can't automatically upgrade it.
     */
    public void test_getPluginChanges_withPluginToUpgrade_returnsNoChanges() throws Exception {
        createPlugin("my-plugin", Version.CURRENT.previousMajor());
        config.setPlugins(List.of(new InstallablePlugin("my-plugin")));

        final PluginChanges pluginChanges = action.getPluginChanges(config, Optional.empty());

        assertThat(pluginChanges.isEmpty(), is(true));
    }

    /**
     * Check that when there is an official plugin in the config file, and that plugin is already installed
     * but needs to be upgraded, then we calculate that the plugin needs to be upgraded.
     */
    public void test_getPluginChanges_withOfficialPluginToUpgrade_returnsPluginToUpgrade() throws Exception {
        createPlugin("analysis-icu", Version.CURRENT.previousMajor());
        config.setPlugins(List.of(new InstallablePlugin("analysis-icu")));

        final PluginChanges pluginChanges = action.getPluginChanges(config, Optional.empty());

        assertThat(pluginChanges.isEmpty(), is(false));
        assertThat(pluginChanges.install, empty());
        assertThat(pluginChanges.remove, empty());
        assertThat(pluginChanges.upgrade, hasSize(1));
        assertThat(pluginChanges.upgrade.get(0).getId(), equalTo("analysis-icu"));
    }

    /**
     * Check that if an unofficial plugins' location has not changed in the cached config, then we
     * calculate that the plugin does not need to be upgraded.
     */
    public void test_getPluginChanges_withCachedConfigAndNoChanges_returnsNoChanges() throws Exception {
        createPlugin("my-plugin");
        config.setPlugins(List.of(new InstallablePlugin("my-plugin", "file://plugin.zip")));

        final PluginsConfig cachedConfig = new PluginsConfig();
        cachedConfig.setPlugins(List.of(new InstallablePlugin("my-plugin", "file://plugin.zip")));

        final PluginChanges pluginChanges = action.getPluginChanges(config, Optional.of(cachedConfig));

        assertThat(pluginChanges.isEmpty(), is(true));
    }

    /**
     * Check that if an unofficial plugins' location has changed, then we calculate that the plugin
     * needs to be upgraded.
     */
    public void test_getPluginChanges_withCachedConfigAndChangedLocation_returnsPluginToUpgrade() throws Exception {
        createPlugin("my-plugin");
        config.setPlugins(List.of(new InstallablePlugin("my-plugin", "file:///after.zip")));

        final PluginsConfig cachedConfig = new PluginsConfig();
        cachedConfig.setPlugins(List.of(new InstallablePlugin("my-plugin", "file://before.zip")));

        final PluginChanges pluginChanges = action.getPluginChanges(config, Optional.of(cachedConfig));

        assertThat(pluginChanges.isEmpty(), is(false));
        assertThat(pluginChanges.install, empty());
        assertThat(pluginChanges.remove, empty());
        assertThat(pluginChanges.upgrade, hasSize(1));
        assertThat(pluginChanges.upgrade.get(0).getId(), equalTo("my-plugin"));
    }

    /**
     * Check that the config file can still specify plugins that have been migrated to modules, but
     * they are ignored.
     */
    public void test_getPluginChanges_withModularisedPluginsToInstall_ignoresPlugins() throws Exception {
        config.setPlugins(
            List.of(
                new InstallablePlugin("repository-azure"),
                new InstallablePlugin("repository-gcs"),
                new InstallablePlugin("repository-s3")
            )
        );

        final PluginChanges pluginChanges = action.getPluginChanges(config, Optional.empty());

        assertThat(pluginChanges.isEmpty(), is(true));
        for (String plugin : List.of("repository-azure", "repository-gcs", "repository-s3")) {
            assertThat(
                terminal.getErrorOutput(),
                containsString(
                    "[" + plugin + "] is no longer a plugin but instead a module packaged with this distribution of Elasticsearch"
                )
            );
        }
    }

    /**
     * Check that if there are plugins already installed that have been migrated to modules, then they are removed,
     * even if they are specified in the config file.
     */
    public void test_getPluginChanges_withModularisedPluginsToRemove_removesPlugins() throws Exception {
        createPlugin("repository-azure");
        createPlugin("repository-gcs");
        createPlugin("repository-s3");
        config.setPlugins(
            List.of(
                new InstallablePlugin("repository-azure"),
                new InstallablePlugin("repository-gcs"),
                new InstallablePlugin("repository-s3")
            )
        );

        final PluginChanges pluginChanges = action.getPluginChanges(config, Optional.empty());

        assertThat(pluginChanges.isEmpty(), is(false));
        assertThat(pluginChanges.install, empty());
        assertThat(pluginChanges.remove, hasSize(3));
        assertThat(pluginChanges.upgrade, empty());
        assertThat(pluginChanges.remove.get(0).getId(), equalTo("repository-azure"));
        assertThat(pluginChanges.remove.get(1).getId(), equalTo("repository-gcs"));
        assertThat(pluginChanges.remove.get(2).getId(), equalTo("repository-s3"));
    }

    /**
     * Check that if there are no changes to apply, then the install and remove actions are not used.
     * This is a redundant test, really, because the sync action exits early if there are no
     * changes.
     */
    public void test_performSync_withNoChanges_doesNothing() throws Exception {
        final InstallPluginAction installAction = mock(InstallPluginAction.class);
        final RemovePluginAction removeAction = mock(RemovePluginAction.class);

        action.performSync(installAction, removeAction, new PluginChanges(List.of(), List.of(), List.of()));

        verify(installAction, never()).execute(anyList());
        verify(removeAction, never()).execute(anyList());
    }

    /**
     * Check that if there are plugins to remove, then the remove action is used.
     */
    public void test_performSync_withPluginsToRemove_callsRemoveAction() throws Exception {
        final InstallPluginAction installAction = mock(InstallPluginAction.class);
        final RemovePluginAction removeAction = mock(RemovePluginAction.class);
        final List<InstallablePlugin> installablePlugins = List.of(new InstallablePlugin("plugin1"), new InstallablePlugin("plugin2"));

        action.performSync(installAction, removeAction, new PluginChanges(installablePlugins, List.of(), List.of()));

        verify(installAction, never()).execute(anyList());
        verify(removeAction).setPurge(true);
        verify(removeAction).execute(installablePlugins);
    }

    /**
     * Check that if there are plugins to install, then the install action is used.
     */
    public void test_performSync_withPluginsToInstall_callsInstallAction() throws Exception {
        final InstallPluginAction installAction = mock(InstallPluginAction.class);
        final RemovePluginAction removeAction = mock(RemovePluginAction.class);
        final List<InstallablePlugin> installablePlugins = List.of(new InstallablePlugin("plugin1"), new InstallablePlugin("plugin2"));

        action.performSync(installAction, removeAction, new PluginChanges(List.of(), installablePlugins, List.of()));

        verify(installAction).execute(installablePlugins);
        verify(removeAction, never()).execute(anyList());
    }

    /**
     * Check that if there are plugins to upgrade, then both the install and remove actions are used.
     */
    public void test_performSync_withPluginsToUpgrade_callsRemoveAndInstallAction() throws Exception {
        final InstallPluginAction installAction = mock(InstallPluginAction.class);
        final RemovePluginAction removeAction = mock(RemovePluginAction.class);
        final InOrder inOrder = Mockito.inOrder(removeAction, installAction);

        final List<InstallablePlugin> installablePlugins = List.of(new InstallablePlugin("plugin1"), new InstallablePlugin("plugin2"));

        action.performSync(installAction, removeAction, new PluginChanges(List.of(), List.of(), installablePlugins));

        inOrder.verify(removeAction).setPurge(false);
        inOrder.verify(removeAction).execute(installablePlugins);
        inOrder.verify(installAction).execute(installablePlugins);
    }

    /**
     * Check that if there are plugins to remove, install and upgrade, then we do everything.
     */
    public void test_performSync_withPluginsToUpgrade_callsUpgradeAction() throws Exception {
        final InstallPluginAction installAction = mock(InstallPluginAction.class);
        final RemovePluginAction removeAction = mock(RemovePluginAction.class);
        final InOrder inOrder = Mockito.inOrder(removeAction, installAction);

        final List<InstallablePlugin> pluginsToRemove = List.of(new InstallablePlugin("plugin1"));
        final List<InstallablePlugin> pluginsToInstall = List.of(new InstallablePlugin("plugin2"));
        final List<InstallablePlugin> pluginsToUpgrade = List.of(new InstallablePlugin("plugin3"));

        action.performSync(installAction, removeAction, new PluginChanges(pluginsToRemove, pluginsToInstall, pluginsToUpgrade));

        inOrder.verify(removeAction).setPurge(true);
        inOrder.verify(removeAction).execute(pluginsToRemove);

        inOrder.verify(installAction).execute(pluginsToInstall);

        inOrder.verify(removeAction).setPurge(false);
        inOrder.verify(removeAction).execute(pluginsToUpgrade);
        inOrder.verify(installAction).execute(pluginsToUpgrade);
    }

    private void createPlugin(String name) throws IOException {
        createPlugin(name, Version.CURRENT);
    }

    private void createPlugin(String name, Version version) throws IOException {
        PluginTestUtil.writePluginProperties(
            env.pluginsFile().resolve(name),
            "description",
            "dummy",
            "name",
            name,
            "version",
            "1.0",
            "elasticsearch.version",
            version.toString(),
            "java.version",
            System.getProperty("java.specification.version"),
            "classname",
            "SomeClass"
        );
    }
}
