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

package org.elasticsearch.action.admin.cluster.settings;

import org.elasticsearch.action.support.master.AcknowledgedRequestBuilder;
import org.elasticsearch.client.ElasticsearchClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.XContentType;

import java.util.Map;

/**
 * Builder for a cluster update settings request
 */
public class ClusterUpdateSettingsRequestBuilder extends AcknowledgedRequestBuilder<ClusterUpdateSettingsRequest,
        ClusterUpdateSettingsResponse, ClusterUpdateSettingsRequestBuilder> {

    public ClusterUpdateSettingsRequestBuilder(ElasticsearchClient client, ClusterUpdateSettingsAction action) {
        super(client, action, new ClusterUpdateSettingsRequest());
    }

    /**
     * Sets the transient settings to be updated. They will not survive a full cluster restart
     */
    public ClusterUpdateSettingsRequestBuilder setTransientSettings(Settings settings) {
        request.transientSettings(settings);
        return this;
    }

    /**
     * Sets the transient settings to be updated. They will not survive a full cluster restart
     */
    public ClusterUpdateSettingsRequestBuilder setTransientSettings(Settings.Builder settings) {
        request.transientSettings(settings);
        return this;
    }

    /**
     * Sets the source containing the transient settings to be updated. They will not survive a full cluster restart
     */
    public ClusterUpdateSettingsRequestBuilder setTransientSettings(String settings, XContentType xContentType) {
        request.transientSettings(settings, xContentType);
        return this;
    }

    /**
     * Sets the transient settings to be updated. They will not survive a full cluster restart
     */
    public ClusterUpdateSettingsRequestBuilder setTransientSettings(Map<String, ?> settings) {
        request.transientSettings(settings);
        return this;
    }

    /**
     * Sets the persistent settings to be updated. They will get applied cross restarts
     */
    public ClusterUpdateSettingsRequestBuilder setPersistentSettings(Settings settings) {
        request.persistentSettings(settings);
        return this;
    }

    /**
     * Sets the persistent settings to be updated. They will get applied cross restarts
     */
    public ClusterUpdateSettingsRequestBuilder setPersistentSettings(Settings.Builder settings) {
        request.persistentSettings(settings);
        return this;
    }

    /**
     * Sets the source containing the persistent settings to be updated. They will get applied cross restarts
     */
    public ClusterUpdateSettingsRequestBuilder setPersistentSettings(String settings, XContentType xContentType) {
        request.persistentSettings(settings, xContentType);
        return this;
    }

    /**
     * Sets the persistent settings to be updated. They will get applied cross restarts
     */
    public ClusterUpdateSettingsRequestBuilder setPersistentSettings(Map<String, ?> settings) {
        request.persistentSettings(settings);
        return this;
    }
}
