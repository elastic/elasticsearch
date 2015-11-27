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

package org.elasticsearch.common.settings;

import org.elasticsearch.common.component.AbstractComponent;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.logging.ESLoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.function.BiConsumer;
import java.util.function.Consumer;
import java.util.function.Predicate;

/**
 * A service that allows to register for node settings change that can come from cluster
 * events holding new settings.
 */
public final class ClusterSettingsService extends SettingsService {
    private final ClusterSettings clusterSettings;

    @Inject
    public ClusterSettingsService(Settings settings, ClusterSettings clusterSettings) {
        super(settings);
        this.clusterSettings = clusterSettings;
    }

    protected Setting<?> getSetting(String key) {
        return this.clusterSettings.get(key);
    }

    public ClusterSettings getClusterSettings() {
        return clusterSettings;
    }
}
