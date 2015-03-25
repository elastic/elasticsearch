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

package org.elasticsearch.index.settings;

import org.elasticsearch.cluster.metadata.IndexMetaData;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.AbstractIndexComponent;
import org.elasticsearch.index.Index;

import java.util.concurrent.CopyOnWriteArrayList;

/**
 * A holds to the latest, updated settings for an index.
 */
public class IndexSettingsService extends AbstractIndexComponent {

    private volatile Settings settings;

    private final CopyOnWriteArrayList<Listener> listeners = new CopyOnWriteArrayList<>();

    @Inject
    public IndexSettingsService(Index index, Settings settings) {
        super(index, settings);
        this.settings = settings;
    }

    public synchronized void refreshSettings(Settings settings) {
        // this.settings include also the node settings
        if (this.settings.getByPrefix(IndexMetaData.INDEX_SETTING_PREFIX).getAsMap().equals(settings.getByPrefix(IndexMetaData.INDEX_SETTING_PREFIX).getAsMap())) {
            // nothing to update, same settings
            return;
        }
        this.settings = ImmutableSettings.settingsBuilder().put(this.settings).put(settings).build();
        for (Listener listener : listeners) {
            try {
                listener.onRefreshSettings(settings);
            } catch (Exception e) {
                logger.warn("failed to refresh settings for [{}]", e, listener);
            }
        }
    }

    public Settings getSettings() {
        return this.settings;
    }

    /**
     * Only settings registered in {@link IndexDynamicSettingsModule} can be changed dynamically.
     */
    public void addListener(Listener listener) {
        this.listeners.add(listener);
    }

    public void removeListener(Listener listener) {
        this.listeners.remove(listener);
    }

    public static interface Listener {
        void onRefreshSettings(Settings settings);
    }
}