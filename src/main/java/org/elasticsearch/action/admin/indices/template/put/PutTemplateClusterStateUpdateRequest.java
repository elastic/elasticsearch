/*
 * Licensed to ElasticSearch and Shay Banon under one
 * or more contributor license agreements. See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. ElasticSearch licenses this
 * file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

package org.elasticsearch.action.admin.indices.template.put;

import com.google.common.collect.Maps;
import org.elasticsearch.action.support.master.ClusterStateUpdateRequest;
import org.elasticsearch.cluster.metadata.IndexMetaData;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.common.settings.Settings;

import java.util.Locale;
import java.util.Map;

/**
 * Cluster state update request that allows to create an index template
 */
public class PutTemplateClusterStateUpdateRequest extends ClusterStateUpdateRequest<PutTemplateClusterStateUpdateRequest> {

    private final String name;
    private final String cause;
    private boolean create;
    private int order;
    private String template;
    private Settings settings = ImmutableSettings.Builder.EMPTY_SETTINGS;
    private Map<String, String> mappings = Maps.newHashMap();
    private Map<String, IndexMetaData.Custom> customs = Maps.newHashMap();

    PutTemplateClusterStateUpdateRequest(String name, String cause) {
        this.name = name;
        this.cause = cause;
    }

    public String name() {
        return name;
    }

    public String cause() {
        return cause;
    }

    public boolean create() {
        return create;
    }

    public PutTemplateClusterStateUpdateRequest create(boolean create) {
        this.create = create;
        return this;
    }

    public int order() {
        return order;
    }

    public PutTemplateClusterStateUpdateRequest order(int order) {
        this.order = order;
        return this;
    }

    public String template() {
        return template;
    }

    public PutTemplateClusterStateUpdateRequest template(String template) {
        this.template = template;
        return this;
    }

    public Settings settings() {
        return settings;
    }

    public PutTemplateClusterStateUpdateRequest settings(Settings settings) {
        this.settings = settings;
        return this;
    }

    public Map<String, String> mappings() {
        return mappings;
    }

    public PutTemplateClusterStateUpdateRequest mappings(Map<String, String> mappings) {
        this.mappings = mappings;
        return this;
    }

    public Map<String, IndexMetaData.Custom> customs() {
        return customs;
    }

    public PutTemplateClusterStateUpdateRequest customs(Map<String, IndexMetaData.Custom> customs) {
        this.customs = customs;
        return this;
    }

    @Override
    public String toString() {
        return String.format(Locale.ROOT, "put index template: name [%s], cause [%s], template [%s]", name, cause, template);
    }
}
