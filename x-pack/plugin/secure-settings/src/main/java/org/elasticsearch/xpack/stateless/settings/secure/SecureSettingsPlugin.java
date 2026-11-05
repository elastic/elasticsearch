/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.stateless.settings.secure;

import org.elasticsearch.action.admin.cluster.settings.UpdateSecureSettingsAction;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.NamedDiff;
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.settings.SecureSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.plugins.ActionPlugin;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.plugins.ReloadablePlugin;
import org.elasticsearch.plugins.internal.ReloadAwarePlugin;
import org.elasticsearch.plugins.internal.SecureSettingsReloadPlugin;
import org.elasticsearch.xcontent.NamedXContentRegistry;
import org.elasticsearch.xcontent.ParseField;
import org.elasticsearch.xpack.encryption.spi.EncryptionServiceRegistry;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

public class SecureSettingsPlugin extends Plugin implements ReloadAwarePlugin, SecureSettingsReloadPlugin, ActionPlugin {

    private ClusterStateSecretsListener clusterStateSecretsListener;
    private SecureSettingsReloader reloader;
    private ClusterService clusterService;
    private Settings nodeSettings;

    @Override
    public Collection<?> createComponents(PluginServices services) {
        this.clusterService = services.clusterService();
        this.nodeSettings = services.environment().settings();
        if (secureSettingsApiEnabled()) {
            this.reloader = new SecureSettingsReloader(
                nodeSettings,
                // Settings#getSecureSettings is package-private
                Settings.builder().put(nodeSettings, true).getSecureSettings(),
                EncryptionServiceRegistry.getEncryptionService(),
                services.threadPool().generic()
            );
        }
        this.clusterStateSecretsListener = new ClusterStateSecretsListener(clusterService, services.environment(), reloader);
        return List.of(clusterStateSecretsListener);
    }

    @Override
    public void setReloadCallback(ReloadablePlugin reloadablePlugin) {
        this.clusterStateSecretsListener.setReloadCallback(reloadablePlugin);
        if (reloader != null) {
            reloader.setReloadCallback(reloadablePlugin);
        }
    }

    @Override
    public void reloadWithKeystore(SecureSettings keystore, ReloadablePlugin reloadablePlugins) throws Exception {
        if (reloader == null) {
            reloadablePlugins.reload(Settings.builder().put(nodeSettings, false).setSecureSettings(keystore).build());
        } else {
            reloader.reloadWithKeystore(keystore, SecureSettingsReloader.Sources.of(clusterService.state()), reloadablePlugins);
        }
    }

    /**
     * Retained for rolling-upgrade compatibility so upgraded nodes can deserialize cluster state
     * from non-upgraded masters that still contain {@link ClusterStateSecretsMetadata}.
     * TODO Remove once all nodes are upgraded (ES-13910).
     */
    @Override
    public List<NamedWriteableRegistry.Entry> getNamedWriteables() {
        List<NamedWriteableRegistry.Entry> entries = new ArrayList<>();
        entries.add(
            new NamedWriteableRegistry.Entry(ClusterState.Custom.class, ClusterStateSecretsMetadata.TYPE, ClusterStateSecretsMetadata::new)
        );
        entries.add(
            new NamedWriteableRegistry.Entry(NamedDiff.class, ClusterStateSecretsMetadata.TYPE, ClusterStateSecretsMetadata::readDiffFrom)
        );
        if (secureSettingsApiEnabled()) {
            entries.addAll(SecureSettingsMetadata.NAMED_WRITEABLES);
        }
        return entries;
    }

    @Override
    public List<NamedXContentRegistry.Entry> getNamedXContent() {
        if (secureSettingsApiEnabled() == false) {
            return List.of();
        }
        return List.of(
            new NamedXContentRegistry.Entry(
                Metadata.ProjectCustom.class,
                new ParseField(SecureSettingsMetadata.TYPE),
                SecureSettingsMetadata::fromXContent
            )
        );
    }

    @Override
    public Collection<ActionHandler> getActions() {
        if (secureSettingsApiEnabled() == false) {
            return List.of();
        }
        return List.of(new ActionHandler(UpdateSecureSettingsAction.INSTANCE, TransportUpdateSecureSettingsAction.class));
    }

    private static boolean secureSettingsApiEnabled() {
        return UpdateSecureSettingsAction.SECURE_SETTINGS_API_FEATURE_FLAG.isEnabled();
    }
}
