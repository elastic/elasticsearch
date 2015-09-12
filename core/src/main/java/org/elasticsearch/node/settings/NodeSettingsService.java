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

package org.elasticsearch.node.settings;

import org.elasticsearch.cluster.ClusterChangedEvent;
import org.elasticsearch.cluster.ClusterService;
import org.elasticsearch.cluster.ClusterStateListener;
import org.elasticsearch.common.component.AbstractComponent;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.logging.ESLoggerFactory;
import org.elasticsearch.common.settings.Settings;

import java.util.Map;
import java.util.concurrent.CopyOnWriteArrayList;

/**
 * A service that allows to register for node settings change that can come from cluster
 * events holding new settings.
 */
public class NodeSettingsService extends AbstractComponent implements ClusterStateListener {

    private static volatile Settings globalSettings = Settings.Builder.EMPTY_SETTINGS;

    /**
     * Returns the global (static) settings last updated by a node. Note, if you have multiple
     * nodes on the same JVM, it will just return the latest one set...
     */
    public static Settings getGlobalSettings() {
        return globalSettings;
    }

    private volatile Settings lastSettingsApplied;

    private final CopyOnWriteArrayList<Listener> listeners = new CopyOnWriteArrayList<>();

    @Inject
    public NodeSettingsService(Settings settings) {
        super(settings);
        globalSettings = settings;
    }

    // inject it as a member, so we won't get into possible cyclic problems
    public void setClusterService(ClusterService clusterService) {
        clusterService.add(this);
    }

    @Override
    public void clusterChanged(ClusterChangedEvent event) {
        // nothing to do until we actually recover from the gateway or any other block indicates we need to disable persistency
        if (event.state().blocks().disableStatePersistence()) {
            return;
        }

        if (!event.metaDataChanged()) {
            // nothing changed in the metadata, no need to check
            return;
        }

        if (lastSettingsApplied != null && event.state().metaData().settings().equals(lastSettingsApplied)) {
            // nothing changed in the settings, ignore
            return;
        }

        for (Listener listener : listeners) {
            try {
                listener.onRefreshSettings(event.state().metaData().settings());
            } catch (Exception e) {
                logger.warn("failed to refresh settings for [{}]", e, listener);
            }
        }

        try {
            for (Map.Entry<String, String> entry : event.state().metaData().settings().getAsMap().entrySet()) {
                if (entry.getKey().startsWith("logger.")) {
                    String component = entry.getKey().substring("logger.".length());
                    if ("_root".equals(component)) {
                        ESLoggerFactory.getRootLogger().setLevel(entry.getValue());
                    } else {
                        ESLoggerFactory.getLogger(component).setLevel(entry.getValue());
                    }
                }
            }
        } catch (Exception e) {
            logger.warn("failed to refresh settings for [{}]", e, "logger");
        }

        lastSettingsApplied = event.state().metaData().settings();
        globalSettings = lastSettingsApplied;
    }

    /**
     * Only settings registered in {@link org.elasticsearch.cluster.ClusterModule} can be changed dynamically.
     */
    public void addListener(Listener listener) {
        this.listeners.add(listener);
    }

    public void removeListener(Listener listener) {
        this.listeners.remove(listener);
    }

    public interface Listener {
        void onRefreshSettings(Settings settings);
    }
}
