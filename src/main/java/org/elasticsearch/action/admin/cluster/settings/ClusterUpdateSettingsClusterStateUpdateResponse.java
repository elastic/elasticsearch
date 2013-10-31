/*
 * Licensed to ElasticSearch and Shay Banon under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. ElasticSearch licenses this
 * file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
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

package org.elasticsearch.action.admin.cluster.settings;

import org.elasticsearch.action.support.master.ClusterStateUpdateResponse;
import org.elasticsearch.common.settings.Settings;

/**
 * Response obtained from a cluster state update that consisted of an update settings request
 */
public class ClusterUpdateSettingsClusterStateUpdateResponse extends ClusterStateUpdateResponse {

    public ClusterUpdateSettingsClusterStateUpdateResponse(boolean acknowledged) {
        super(acknowledged);
    }

    private Settings transientSettings;
    private Settings persistentSettings;

    public Settings transientSettings() {
        return transientSettings;
    }

    public ClusterUpdateSettingsClusterStateUpdateResponse transientSettings(Settings transientSettings) {
        this.transientSettings = transientSettings;
        return this;
    }

    public Settings persistentSettings() {
        return persistentSettings;
    }

    public ClusterUpdateSettingsClusterStateUpdateResponse persistentSettings(Settings persistentSettings) {
        this.persistentSettings = persistentSettings;
        return this;
    }
}
