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
package org.elasticsearch.cluster.metadata;

import com.carrotsearch.hppc.cursors.ObjectCursor;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.ElasticsearchIllegalArgumentException;
import org.elasticsearch.action.admin.indices.alias.Alias;
import org.elasticsearch.action.support.master.MasterNodeOperationRequest;
import org.elasticsearch.cluster.ClusterService;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.TimeoutClusterStateUpdateTask;
import org.elasticsearch.common.Priority;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.component.AbstractComponent;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.regex.Regex;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.indices.IndexTemplateAlreadyExistsException;
import org.elasticsearch.indices.IndexTemplateMissingException;
import org.elasticsearch.indices.InvalidIndexTemplateException;

import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;

/**
 * Service responsible for submitting index templates updates
 */
public class MetaDataIndexTemplateService extends AbstractComponent {

    private final ClusterService clusterService;
    private final AliasValidator aliasValidator;

    @Inject
    public MetaDataIndexTemplateService(Settings settings, ClusterService clusterService, AliasValidator aliasValidator) {
        super(settings);
        this.clusterService = clusterService;
        this.aliasValidator = aliasValidator;
    }

    public void removeTemplates(final RemoveRequest request, final RemoveListener listener) {
        clusterService.submitStateUpdateTask("remove-index-template [" + request.name + "]", Priority.URGENT, new TimeoutClusterStateUpdateTask() {

            @Override
            public TimeValue timeout() {
                return request.masterTimeout;
            }

            @Override
            public void onFailure(String source, Throwable t) {
                listener.onFailure(t);
            }

            @Override
            public ClusterState execute(ClusterState currentState) {
                Set<String> templateNames = Sets.newHashSet();
                for (ObjectCursor<String> cursor : currentState.metaData().templates().keys()) {
                    String templateName = cursor.value;
                    if (Regex.simpleMatch(request.name, templateName)) {
                        templateNames.add(templateName);
                    }
                }
                if (templateNames.isEmpty()) {
                    // if its a match all pattern, and no templates are found (we have none), don't
                    // fail with index missing...
                    if (Regex.isMatchAllPattern(request.name)) {
                        return currentState;
                    }
                    throw new IndexTemplateMissingException(request.name);
                }
                MetaData.Builder metaData = MetaData.builder(currentState.metaData());
                for (String templateName : templateNames) {
                    metaData.removeTemplate(templateName);
                }
                return ClusterState.builder(currentState).metaData(metaData).build();
            }

            @Override
            public void clusterStateProcessed(String source, ClusterState oldState, ClusterState newState) {
                listener.onResponse(new RemoveResponse(true));
            }
        });
    }

    public void putTemplate(final PutRequest request, final PutListener listener) {
        ImmutableSettings.Builder updatedSettingsBuilder = ImmutableSettings.settingsBuilder();
        updatedSettingsBuilder.put(request.settings).normalizePrefix(IndexMetaData.INDEX_SETTING_PREFIX);
        request.settings(updatedSettingsBuilder.build());

        if (request.name == null) {
            listener.onFailure(new ElasticsearchIllegalArgumentException("index_template must provide a name"));
            return;
        }
        if (request.template == null) {
            listener.onFailure(new ElasticsearchIllegalArgumentException("index_template must provide a template"));
            return;
        }

        try {
            validate(request);
        } catch (Throwable e) {
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
            for (Alias alias : request.aliases) {
                AliasMetaData aliasMetaData = AliasMetaData.builder(alias.name()).filter(alias.filter())
                        .indexRouting(alias.indexRouting()).searchRouting(alias.searchRouting()).build();
                templateBuilder.putAlias(aliasMetaData);
            }
            for (Map.Entry<String, IndexMetaData.Custom> entry : request.customs.entrySet()) {
                templateBuilder.putCustom(entry.getKey(), entry.getValue());
            }
        } catch (Throwable e) {
            listener.onFailure(e);
            return;
        }
        final IndexTemplateMetaData template = templateBuilder.build();

        clusterService.submitStateUpdateTask("create-index-template [" + request.name + "], cause [" + request.cause + "]", Priority.URGENT, new TimeoutClusterStateUpdateTask() {

            @Override
            public TimeValue timeout() {
                return request.masterTimeout;
            }

            @Override
            public void onFailure(String source, Throwable t) {
                listener.onFailure(t);
            }

            @Override
            public ClusterState execute(ClusterState currentState) {
                if (request.create && currentState.metaData().templates().containsKey(request.name)) {
                    throw new IndexTemplateAlreadyExistsException(request.name);
                }
                MetaData.Builder builder = MetaData.builder(currentState.metaData()).put(template);

                return ClusterState.builder(currentState).metaData(builder).build();
            }

            @Override
            public void clusterStateProcessed(String source, ClusterState oldState, ClusterState newState) {
                listener.onResponse(new PutResponse(true, template));
            }
        });
    }

    private void validate(PutRequest request) throws ElasticsearchException {
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
        if (!request.name.toLowerCase(Locale.ROOT).equals(request.name)) {
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
        if (!Strings.validFileNameExcludingAstrix(request.template)) {
            throw new InvalidIndexTemplateException(request.name, "template must not container the following characters " + Strings.INVALID_FILENAME_CHARS);
        }

        for (Alias alias : request.aliases) {
            //we validate the alias only partially, as we don't know yet to which index it'll get applied to
            aliasValidator.validateAliasStandalone(alias);
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
        List<Alias> aliases = Lists.newArrayList();
        Map<String, IndexMetaData.Custom> customs = Maps.newHashMap();

        TimeValue masterTimeout = MasterNodeOperationRequest.DEFAULT_MASTER_NODE_TIMEOUT;

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
        
        public PutRequest aliases(Set<Alias> aliases) {
            this.aliases.addAll(aliases);
            return this;
        }

        public PutRequest customs(Map<String, IndexMetaData.Custom> customs) {
            this.customs.putAll(customs);
            return this;
        }

        public PutRequest putMapping(String mappingType, String mappingSource) {
            mappings.put(mappingType, mappingSource);
            return this;
        }

        public PutRequest masterTimeout(TimeValue masterTimeout) {
            this.masterTimeout = masterTimeout;
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
        TimeValue masterTimeout = MasterNodeOperationRequest.DEFAULT_MASTER_NODE_TIMEOUT;

        public RemoveRequest(String name) {
            this.name = name;
        }

        public RemoveRequest masterTimeout(TimeValue masterTimeout) {
            this.masterTimeout = masterTimeout;
            return this;
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
