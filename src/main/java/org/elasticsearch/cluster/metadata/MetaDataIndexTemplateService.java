/*
 * Licensed to Elastic Search and Shay Banon under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. Elastic Search licenses this
 * file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.elasticsearch.cluster.metadata;

import com.google.common.collect.Maps;
import org.elasticsearch.ElasticSearchException;
import org.elasticsearch.ElasticSearchIllegalArgumentException;
import org.elasticsearch.cluster.ClusterService;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.ProcessedClusterStateUpdateTask;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.component.AbstractComponent;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.indices.IndexTemplateAlreadyExistsException;
import org.elasticsearch.indices.IndexTemplateMissingException;
import org.elasticsearch.indices.InvalidIndexTemplateException;

import java.util.Map;

/**
 *
 */
public class MetaDataIndexTemplateService extends AbstractComponent {

    private final ClusterService clusterService;

    @Inject
    public MetaDataIndexTemplateService(Settings settings, ClusterService clusterService) {
        super(settings);
        this.clusterService = clusterService;
    }

    public void removeTemplate(final RemoveRequest request, final RemoveListener listener) {
        clusterService.submitStateUpdateTask("remove-index-template [" + request.name + "]", new ProcessedClusterStateUpdateTask() {
            @Override
            public ClusterState execute(ClusterState currentState) {
                if (!currentState.metaData().templates().containsKey(request.name)) {
                    listener.onFailure(new IndexTemplateMissingException(request.name));
                    return currentState;
                }
                MetaData.Builder metaData = MetaData.builder().metaData(currentState.metaData())
                        .removeTemplate(request.name);

                return ClusterState.builder().state(currentState).metaData(metaData).build();
            }

            @Override
            public void clusterStateProcessed(ClusterState clusterState) {
                listener.onResponse(new RemoveResponse(true));
            }
        });
    }

    public void putTemplate(final PutRequest request, final PutListener listener) {
        ImmutableSettings.Builder updatedSettingsBuilder = ImmutableSettings.settingsBuilder();
        for (Map.Entry<String, String> entry : request.settings.getAsMap().entrySet()) {
            if (!entry.getKey().startsWith("index.")) {
                updatedSettingsBuilder.put("index." + entry.getKey(), entry.getValue());
            } else {
                updatedSettingsBuilder.put(entry.getKey(), entry.getValue());
            }
        }
        request.settings(updatedSettingsBuilder.build());

        if (request.name == null) {
            listener.onFailure(new ElasticSearchIllegalArgumentException("index_template must provide a name"));
            return;
        }
        if (request.template == null) {
            listener.onFailure(new ElasticSearchIllegalArgumentException("index_template must provide a template"));
            return;
        }

        try {
            validate(request);
        } catch (Exception e) {
            listener.onFailure(e);
            return;
        }

        IndexTemplateMetaData.Builder templateBuilder;
        try {
            templateBuilder = IndexTemplateMetaData.builder(request.name);
            templateBuilder.order(request.order);
            templateBuilder.template(request.template);
            templateBuilder.settings(request.settings);
            for (Map.Entry<String, String> entry : request.mappings.entrySet()) {
                templateBuilder.putMapping(entry.getKey(), entry.getValue());
            }
        } catch (Exception e) {
            listener.onFailure(e);
            return;
        }
        final IndexTemplateMetaData template = templateBuilder.build();

        clusterService.submitStateUpdateTask("create-index-template [" + request.name + "], cause [" + request.cause + "]", new ProcessedClusterStateUpdateTask() {
            @Override
            public ClusterState execute(ClusterState currentState) {
                if (request.create && currentState.metaData().templates().containsKey(request.name)) {
                    listener.onFailure(new IndexTemplateAlreadyExistsException(request.name));
                    return currentState;
                }
                MetaData.Builder builder = MetaData.builder().metaData(currentState.metaData())
                        .put(template);

                return ClusterState.builder().state(currentState).metaData(builder).build();
            }

            @Override
            public void clusterStateProcessed(ClusterState clusterState) {
                listener.onResponse(new PutResponse(true, template));
            }
        });
    }

    private void validate(PutRequest request) throws ElasticSearchException {
        if (request.name.contains(" ")) {
            throw new InvalidIndexTemplateException(request.name, "name must not contain a space");
        }
        if (request.name.contains(",")) {
            throw new InvalidIndexTemplateException(request.name, "name must not contain a ','");
        }
        if (request.name.contains("#")) {
            throw new InvalidIndexTemplateException(request.name, "name must not contain a '#'");
        }
        if (request.name.startsWith("_")) {
            throw new InvalidIndexTemplateException(request.name, "name must not start with '_'");
        }
        if (!request.name.toLowerCase().equals(request.name)) {
            throw new InvalidIndexTemplateException(request.name, "name must be lower cased");
        }
        if (request.template.contains(" ")) {
            throw new InvalidIndexTemplateException(request.name, "template must not contain a space");
        }
        if (request.template.contains(",")) {
            throw new InvalidIndexTemplateException(request.name, "template must not contain a ','");
        }
        if (request.template.contains("#")) {
            throw new InvalidIndexTemplateException(request.name, "template must not contain a '#'");
        }
        if (request.template.startsWith("_")) {
            throw new InvalidIndexTemplateException(request.name, "template must not start with '_'");
        }
        if (!request.name.toLowerCase().equals(request.name)) {
            throw new InvalidIndexTemplateException(request.name, "template must be lower cased");
        }
        if (!Strings.validFileNameExcludingAstrix(request.template)) {
            throw new InvalidIndexTemplateException(request.name, "template must not container the following characters " + Strings.INVALID_FILENAME_CHARS);
        }
    }

    public static interface PutListener {

        void onResponse(PutResponse response);

        void onFailure(Throwable t);
    }

    public static class PutRequest {
        final String name;
        final String cause;
        boolean create;
        int order;
        String template;
        Settings settings = ImmutableSettings.Builder.EMPTY_SETTINGS;
        Map<String, String> mappings = Maps.newHashMap();

        public PutRequest(String cause, String name) {
            this.cause = cause;
            this.name = name;
        }

        public PutRequest order(int order) {
            this.order = order;
            return this;
        }

        public PutRequest template(String template) {
            this.template = template;
            return this;
        }

        public PutRequest create(boolean create) {
            this.create = create;
            return this;
        }

        public PutRequest settings(Settings settings) {
            this.settings = settings;
            return this;
        }

        public PutRequest mappings(Map<String, String> mappings) {
            this.mappings.putAll(mappings);
            return this;
        }

        public PutRequest putMapping(String mappingType, String mappingSource) {
            mappings.put(mappingType, mappingSource);
            return this;
        }
    }

    public static class PutResponse {
        private final boolean acknowledged;
        private final IndexTemplateMetaData template;

        public PutResponse(boolean acknowledged, IndexTemplateMetaData template) {
            this.acknowledged = acknowledged;
            this.template = template;
        }

        public boolean acknowledged() {
            return acknowledged;
        }

        public IndexTemplateMetaData template() {
            return template;
        }
    }

    public static class RemoveRequest {
        final String name;

        public RemoveRequest(String name) {
            this.name = name;
        }
    }

    public static class RemoveResponse {
        private final boolean acknowledged;

        public RemoveResponse(boolean acknowledged) {
            this.acknowledged = acknowledged;
        }

        public boolean acknowledged() {
            return acknowledged;
        }
    }

    public static interface RemoveListener {

        void onResponse(RemoveResponse response);

        void onFailure(Throwable t);
    }
}
