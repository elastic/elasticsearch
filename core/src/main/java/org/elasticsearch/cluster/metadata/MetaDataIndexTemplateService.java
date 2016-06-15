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
import org.elasticsearch.Version;
import org.elasticsearch.action.admin.indices.alias.Alias;
import org.elasticsearch.action.support.master.MasterNodeRequest;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.ClusterStateUpdateTask;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.Priority;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.UUIDs;
import org.elasticsearch.common.ValidationException;
import org.elasticsearch.common.component.AbstractComponent;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.regex.Regex;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.NodeServicesProvider;
import org.elasticsearch.index.IndexService;
import org.elasticsearch.index.mapper.MapperParsingException;
import org.elasticsearch.index.mapper.MapperService;
import org.elasticsearch.indices.IndexTemplateAlreadyExistsException;
import org.elasticsearch.indices.IndexTemplateMissingException;
import org.elasticsearch.indices.IndicesService;
import org.elasticsearch.indices.InvalidIndexTemplateException;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
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
    private final IndicesService indicesService;
    private final MetaDataCreateIndexService metaDataCreateIndexService;
    private final NodeServicesProvider nodeServicesProvider;

    @Inject
    public MetaDataIndexTemplateService(Settings settings, ClusterService clusterService, MetaDataCreateIndexService metaDataCreateIndexService, AliasValidator aliasValidator, IndicesService indicesService, NodeServicesProvider nodeServicesProvider) {
        super(settings);
        this.clusterService = clusterService;
        this.aliasValidator = aliasValidator;
        this.indicesService = indicesService;
        this.metaDataCreateIndexService = metaDataCreateIndexService;
        this.nodeServicesProvider = nodeServicesProvider;
    }

    public void removeTemplates(final RemoveRequest request, final RemoveListener listener) {
        clusterService.submitStateUpdateTask("remove-index-template [" + request.name + "]", new ClusterStateUpdateTask(Priority.URGENT) {

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
                Set<String> templateNames = new HashSet<>();
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
        Settings.Builder updatedSettingsBuilder = Settings.builder();
        updatedSettingsBuilder.put(request.settings).normalizePrefix(IndexMetaData.INDEX_SETTING_PREFIX);
        request.settings(updatedSettingsBuilder.build());

        if (request.name == null) {
            listener.onFailure(new IllegalArgumentException("index_template must provide a name"));
            return;
        }
        if (request.template == null) {
            listener.onFailure(new IllegalArgumentException("index_template must provide a template"));
            return;
        }

        try {
            validate(request);
        } catch (Throwable e) {
            listener.onFailure(e);
            return;
        }

        final IndexTemplateMetaData.Builder templateBuilder = IndexTemplateMetaData.builder(request.name);

        clusterService.submitStateUpdateTask("create-index-template [" + request.name + "], cause [" + request.cause + "]",
                new ClusterStateUpdateTask(Priority.URGENT) {

            @Override
            public TimeValue timeout() {
                return request.masterTimeout;
            }

            @Override
            public void onFailure(String source, Throwable t) {
                listener.onFailure(t);
            }

            @Override
            public ClusterState execute(ClusterState currentState) throws Exception {
                if (request.create && currentState.metaData().templates().containsKey(request.name)) {
                    throw new IndexTemplateAlreadyExistsException(request.name);
                }

                validateAndAddTemplate(request, templateBuilder, indicesService, nodeServicesProvider, metaDataCreateIndexService);

                for (Alias alias : request.aliases) {
                    AliasMetaData aliasMetaData = AliasMetaData.builder(alias.name()).filter(alias.filter())
                        .indexRouting(alias.indexRouting()).searchRouting(alias.searchRouting()).build();
                    templateBuilder.putAlias(aliasMetaData);
                }
                for (Map.Entry<String, IndexMetaData.Custom> entry : request.customs.entrySet()) {
                    templateBuilder.putCustom(entry.getKey(), entry.getValue());
                }
                IndexTemplateMetaData template = templateBuilder.build();

                MetaData.Builder builder = MetaData.builder(currentState.metaData()).put(template);

                return ClusterState.builder(currentState).metaData(builder).build();
            }

            @Override
            public void clusterStateProcessed(String source, ClusterState oldState, ClusterState newState) {
                listener.onResponse(new PutResponse(true, templateBuilder.build()));
            }
        });
    }

    private static void validateAndAddTemplate(final PutRequest request, IndexTemplateMetaData.Builder templateBuilder, IndicesService indicesService,
                                               NodeServicesProvider nodeServicesProvider, MetaDataCreateIndexService metaDataCreateIndexService) throws Exception {
        Index createdIndex = null;
        final String temporaryIndexName = UUIDs.randomBase64UUID();
        try {

            //create index service for parsing and validating "mappings"
            Settings dummySettings = Settings.builder()
                .put(IndexMetaData.SETTING_VERSION_CREATED, Version.CURRENT)
                .put(request.settings)
                .put(IndexMetaData.SETTING_NUMBER_OF_SHARDS, 1)
                .put(IndexMetaData.SETTING_NUMBER_OF_REPLICAS, 0)
                .put(IndexMetaData.SETTING_INDEX_UUID, UUIDs.randomBase64UUID())
                .build();

            final IndexMetaData tmpIndexMetadata = IndexMetaData.builder(temporaryIndexName).settings(dummySettings).build();
            IndexService dummyIndexService = indicesService.createIndex(nodeServicesProvider, tmpIndexMetadata, Collections.emptyList());
            createdIndex = dummyIndexService.index();

            templateBuilder.order(request.order);
            templateBuilder.template(request.template);
            templateBuilder.settings(request.settings);

            Map<String, Map<String, Object>> mappingsForValidation = new HashMap<>();
            for (Map.Entry<String, String> entry : request.mappings.entrySet()) {
                try {
                    templateBuilder.putMapping(entry.getKey(), entry.getValue());
                } catch (Exception e) {
                    throw new MapperParsingException("Failed to parse mapping [{}]: {}", e, entry.getKey(), e.getMessage());
                }
                mappingsForValidation.put(entry.getKey(), MapperService.parseMapping(entry.getValue()));
            }

            dummyIndexService.mapperService().merge(mappingsForValidation, false);

        } finally {
            if (createdIndex != null) {
                indicesService.removeIndex(createdIndex, " created for parsing template mapping");
            }
        }
    }

    private void validate(PutRequest request) {
        List<String> validationErrors = new ArrayList<>();
        if (request.name.contains(" ")) {
            validationErrors.add("name must not contain a space");
        }
        if (request.name.contains(",")) {
            validationErrors.add("name must not contain a ','");
        }
        if (request.name.contains("#")) {
            validationErrors.add("name must not contain a '#'");
        }
        if (request.name.startsWith("_")) {
            validationErrors.add("name must not start with '_'");
        }
        if (!request.name.toLowerCase(Locale.ROOT).equals(request.name)) {
            validationErrors.add("name must be lower cased");
        }
        if (request.template.contains(" ")) {
            validationErrors.add("template must not contain a space");
        }
        if (request.template.contains(",")) {
            validationErrors.add("template must not contain a ','");
        }
        if (request.template.contains("#")) {
            validationErrors.add("template must not contain a '#'");
        }
        if (request.template.startsWith("_")) {
            validationErrors.add("template must not start with '_'");
        }
        if (!Strings.validFileNameExcludingAstrix(request.template)) {
            validationErrors.add("template must not contain the following characters " + Strings.INVALID_FILENAME_CHARS);
        }

        List<String> indexSettingsValidation = metaDataCreateIndexService.getIndexSettingsValidationErrors(request.settings);
        validationErrors.addAll(indexSettingsValidation);
        if (!validationErrors.isEmpty()) {
            ValidationException validationException = new ValidationException();
            validationException.addValidationErrors(validationErrors);
            throw new InvalidIndexTemplateException(request.name, validationException.getMessage());
        }

        for (Alias alias : request.aliases) {
            //we validate the alias only partially, as we don't know yet to which index it'll get applied to
            aliasValidator.validateAliasStandalone(alias);
            if (request.template.equals(alias.name())) {
                throw new IllegalArgumentException("Alias [" + alias.name() + "] cannot be the same as the template pattern [" + request.template + "]");
            }
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
        Settings settings = Settings.Builder.EMPTY_SETTINGS;
        Map<String, String> mappings = new HashMap<>();
        List<Alias> aliases = new ArrayList<>();
        Map<String, IndexMetaData.Custom> customs = new HashMap<>();

        TimeValue masterTimeout = MasterNodeRequest.DEFAULT_MASTER_NODE_TIMEOUT;

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
        TimeValue masterTimeout = MasterNodeRequest.DEFAULT_MASTER_NODE_TIMEOUT;

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
