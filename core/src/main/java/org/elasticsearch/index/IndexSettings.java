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

// TODO add javadocs - this also needs a dedicated unit test
public final class IndexSettings {
    private volatile Settings settings;
    private final List<Consumer<Settings>> updateListeners;
    private final Index index;
    private final Version version;
    private final ESLogger logger;

    public IndexSettings(Index index) {
        this(index, Settings.settingsBuilder().put(IndexMetaData.SETTING_VERSION_CREATED, Version.CURRENT).build(), Collections.EMPTY_LIST);
    }

    public IndexSettings(Index index, Settings settings, Collection<Consumer<Settings>> updateListeners) {
        this.settings = settings;
        this.updateListeners = Collections.unmodifiableList(new ArrayList<>(updateListeners));
        this.index = index;
        version = Version.indexCreated(settings);
        logger = Loggers.getLogger(getClass(), settings, index);
    }

    public Settings getSettings() {
        return settings;
    }

    public Index getIndex() {
        return index;
    }

    public String getUUID() {
        return settings.get(IndexMetaData.SETTING_INDEX_UUID, IndexMetaData.INDEX_UUID_NA_VALUE);
    }

    public boolean hasCustomDataPath() {
        return NodeEnvironment.hasCustomDataPath(settings);
    }

    public Version getVersion() {
        return version;
    }

    public String getNodeName() {
        return settings.get("name", "");
    }

    synchronized boolean updateSettings(Settings settings) {
        if (Version.indexCreated(settings) != version) {
            throw new IllegalStateException("version mismatch on settings update");
        }

        if (this.settings.getByPrefix(IndexMetaData.INDEX_SETTING_PREFIX).getAsMap().equals(settings.getByPrefix(IndexMetaData.INDEX_SETTING_PREFIX).getAsMap())) {
            // nothing to update, same settings
            return false;
        }
        this.settings = Settings.builder().put(this.settings).put(settings).build();
        for (final Consumer<Settings> consumer : updateListeners) {
            try {
                consumer.accept(settings);
            } catch (Exception e) {
                logger.warn("failed to refresh index settings for [{}]", e, settings);
            }
        }
        return true;
    }

    List<Consumer<Settings>> getUpdateListeners() { // for testing
        return updateListeners;
    }
}
