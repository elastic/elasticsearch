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
package org.elasticsearch.index;

import org.elasticsearch.Version;
import org.elasticsearch.cluster.metadata.IndexMetaData;
import org.elasticsearch.common.logging.ESLogger;
import org.elasticsearch.common.logging.Loggers;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.env.NodeEnvironment;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.function.Consumer;

/**
 * This class encapsulates all index level settings and handles settings updates.
 * It's created per index and available to all index level classes and allows them to retrieve
 * the latest updated settings instance. Classes that need to listen to settings updates can register
 * a settings consumer at index creation via {@link IndexModule#addIndexSettingsListener(Consumer)} that will
 * be called for each settings update.
 */
public final class IndexSettings {
    private final String uuid;
    private volatile Settings settings;
    private final List<Consumer<Settings>> updateListeners;
    private final Index index;
    private final Version version;
    private final ESLogger logger;
    private final String nodeName;

    /**
     * Creates a new {@link IndexSettings} instance
     * @param index the index this settings object is associated with
     * @param settings the actual settings including the node level settings
     * @param updateListeners a collection of listeners / consumers that should be notified if one or more settings are updated
     */
    public IndexSettings(Index index, Settings settings, Collection<Consumer<Settings>> updateListeners) {
        this.settings = settings;
        this.updateListeners = Collections.unmodifiableList(new ArrayList<>(updateListeners));
        this.index = index;
        version = Version.indexCreated(settings);
        uuid = settings.get(IndexMetaData.SETTING_INDEX_UUID, IndexMetaData.INDEX_UUID_NA_VALUE);
        logger = Loggers.getLogger(getClass(), settings, index);
        nodeName = settings.get("name", "");
    }

    /**
     * Returns the settings for this index. These settings contain the node and index level settings where
     * settings that are specified on both index and node level are overwritten by the index settings.
     */
    public Settings getSettings() {
        return settings;
    }

    /**
     * Returns the index this settings object belongs to
     */
    public Index getIndex() {
        return index;
    }

    /**
     * Returns the indexes UUID
     */
    public String getUUID() {
        return uuid;
    }

    /**
     * Returns <code>true</code> if the index has a custom data path
     */
    public boolean hasCustomDataPath() {
        return NodeEnvironment.hasCustomDataPath(settings);
    }

    /**
     * Returns the version the index was created on.
     * @see Version#indexCreated(Settings)
     */
    public Version getIndexVersionCreated() {
        return version;
    }

    /**
     * Returns the current node name
     */
    public String getNodeName() {
        return nodeName;
    }

    /**
     * Notifies  all registered settings consumers with the new settings iff at least one setting has changed.
     *
     * @return <code>true</code> iff any setting has been updated otherwise <code>false</code>.
     */
    synchronized boolean updateIndexSettings(Settings newSettings) {
        if (Version.indexCreated(newSettings) != version) {
            throw new IllegalArgumentException("version mismatch on settings update expected: " + version + " but was: " + Version.indexCreated(newSettings));
        }
        final String newUUID = newSettings.get(IndexMetaData.SETTING_INDEX_UUID, IndexMetaData.INDEX_UUID_NA_VALUE);
        if (newUUID.equals(getUUID()) == false) {
            throw new IllegalArgumentException("uuid mismatch on settings update expected: " + uuid + " but was: " + newUUID);
        }
        final Settings existingSettings = this.settings;
        if (existingSettings.getByPrefix(IndexMetaData.INDEX_SETTING_PREFIX).getAsMap().equals(newSettings.getByPrefix(IndexMetaData.INDEX_SETTING_PREFIX).getAsMap())) {
            // nothing to update, same settings
            return false;
        }
        this.settings = Settings.builder().put(existingSettings).put(newSettings).build();
        final Settings mergedSettings = this.settings;
        for (final Consumer<Settings> consumer : updateListeners) {
            try {
                consumer.accept(mergedSettings);
            } catch (Exception e) {
                logger.warn("failed to refresh index settings for [{}]", e, mergedSettings);
            }
        }
        return true;
    }

    /**
     * Returns all settings update consumers
     */
    List<Consumer<Settings>> getUpdateListeners() { // for testing
        return updateListeners;
    }
}
