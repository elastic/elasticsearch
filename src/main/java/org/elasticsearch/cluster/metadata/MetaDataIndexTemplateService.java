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

import com.google.common.collect.Sets;
import org.elasticsearch.ElasticSearchException;
import org.elasticsearch.ElasticSearchIllegalArgumentException;
import org.elasticsearch.action.admin.indices.template.delete.DeleteTemplateClusterStateUpdateRequest;
import org.elasticsearch.action.admin.indices.template.put.PutTemplateClusterStateUpdateRequest;
import org.elasticsearch.action.support.master.ClusterStateUpdateListener;
import org.elasticsearch.action.support.master.ClusterStateUpdateResponse;
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

import java.util.Locale;
import java.util.Map;
import java.util.Set;

/**
 * Service responsible for creating and removing index templates
 */
public class MetaDataIndexTemplateService extends AbstractComponent {

    private final ClusterService clusterService;

    @Inject
    public MetaDataIndexTemplateService(Settings settings, ClusterService clusterService) {
        super(settings);
        this.clusterService = clusterService;
    }

    public void removeTemplates(final DeleteTemplateClusterStateUpdateRequest request, final ClusterStateUpdateListener<ClusterStateUpdateResponse> listener) {
        //we don't need to wait for ack when putting a template
        //since templates are internally read when creating an index, which always happens on the master
        clusterService.submitStateUpdateTask("remove-index-template [" + request.name() + "]", Priority.URGENT, new TimeoutClusterStateUpdateTask() {

            @Override
            public TimeValue timeout() {
                return request.masterNodeTimeout();
            }

            @Override
            public void onFailure(String source, Throwable t) {
                listener.onFailure(t);
            }

            @Override
            public ClusterState execute(ClusterState currentState) {
                Set<String> templateNames = Sets.newHashSet();
                for (String templateName : currentState.metaData().templates().keySet()) {
                    if (Regex.simpleMatch(request.name(), templateName)) {
                        templateNames.add(templateName);
                    }
                }
                if (templateNames.isEmpty()) {
                    // if its a match all pattern, and no templates are found (we have none), don't
                    // fail with index missing...
                    if (Regex.isMatchAllPattern(request.name())) {
                        return currentState;
                    }
                    throw new IndexTemplateMissingException(request.name());
                }
                MetaData.Builder metaData = MetaData.builder().metaData(currentState.metaData());
                for (String templateName : templateNames) {
                    metaData.removeTemplate(templateName);
                }
                return ClusterState.builder().state(currentState).metaData(metaData).build();
            }

            @Override
            public void clusterStateProcessed(String source, ClusterState oldState, ClusterState newState) {
                listener.onResponse(new ClusterStateUpdateResponse(true));
            }
        });
    }

    public void putTemplate(final PutTemplateClusterStateUpdateRequest request, final ClusterStateUpdateListener<ClusterStateUpdateResponse> listener) {
        ImmutableSettings.Builder updatedSettingsBuilder = ImmutableSettings.settingsBuilder();
        for (Map.Entry<String, String> entry : request.settings().getAsMap().entrySet()) {
            if (!entry.getKey().startsWith("index.")) {
                updatedSettingsBuilder.put("index." + entry.getKey(), entry.getValue());
            } else {
                updatedSettingsBuilder.put(entry.getKey(), entry.getValue());
            }
        }
        request.settings(updatedSettingsBuilder.build());

        if (request.name() == null) {
            listener.onFailure(new ElasticSearchIllegalArgumentException("index_template must provide a name"));
            return;
        }
        if (request.template() == null) {
            listener.onFailure(new ElasticSearchIllegalArgumentException("index_template must provide a template"));
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
            templateBuilder = IndexTemplateMetaData.builder(request.name());
            templateBuilder.order(request.order());
            templateBuilder.template(request.template());
            templateBuilder.settings(request.settings());
            for (Map.Entry<String, String> entry : request.mappings().entrySet()) {
                templateBuilder.putMapping(entry.getKey(), entry.getValue());
            }
            for (Map.Entry<String, IndexMetaData.Custom> entry : request.customs().entrySet()) {
                templateBuilder.putCustom(entry.getKey(), entry.getValue());
            }
        } catch (Throwable e) {
            listener.onFailure(e);
            return;
        }
        final IndexTemplateMetaData template = templateBuilder.build();
        //we don't need to wait for ack when putting a template
        //since templates are internally read when creating an index, which always happens on the master
        clusterService.submitStateUpdateTask("create-index-template [" + request.name() + "], cause [" + request.cause() + "]", Priority.URGENT, new TimeoutClusterStateUpdateTask() {

            @Override
            public TimeValue timeout() {
                return request.masterNodeTimeout();
            }

            @Override
            public void onFailure(String source, Throwable t) {
                listener.onFailure(t);
            }

            @Override
            public ClusterState execute(ClusterState currentState) {
                if (request.create() && currentState.metaData().templates().containsKey(request.name())) {
                    throw new IndexTemplateAlreadyExistsException(request.name());
                }
                MetaData.Builder builder = MetaData.builder().metaData(currentState.metaData()).put(template);

                return ClusterState.builder().state(currentState).metaData(builder).build();
            }

            @Override
            public void clusterStateProcessed(String source, ClusterState oldState, ClusterState newState) {
                listener.onResponse(new ClusterStateUpdateResponse(true));
            }
        });
    }

    private void validate(PutTemplateClusterStateUpdateRequest request) throws ElasticSearchException {
        if (request.name().contains(" ")) {
            throw new InvalidIndexTemplateException(request.name(), "name must not contain a space");
        }
        if (request.name().contains(",")) {
            throw new InvalidIndexTemplateException(request.name(), "name must not contain a ','");
        }
        if (request.name().contains("#")) {
            throw new InvalidIndexTemplateException(request.name(), "name must not contain a '#'");
        }
        if (request.name().startsWith("_")) {
            throw new InvalidIndexTemplateException(request.name(), "name must not start with '_'");
        }
        if (!request.name().toLowerCase(Locale.ROOT).equals(request.name())) {
            throw new InvalidIndexTemplateException(request.name(), "name must be lower cased");
        }
        if (request.template().contains(" ")) {
            throw new InvalidIndexTemplateException(request.name(), "template must not contain a space");
        }
        if (request.template().contains(",")) {
            throw new InvalidIndexTemplateException(request.name(), "template must not contain a ','");
        }
        if (request.template().contains("#")) {
            throw new InvalidIndexTemplateException(request.name(), "template must not contain a '#'");
        }
        if (request.template().startsWith("_")) {
            throw new InvalidIndexTemplateException(request.name(), "template must not start with '_'");
        }
        if (!Strings.validFileNameExcludingAstrix(request.template())) {
            throw new InvalidIndexTemplateException(request.name(), "template must not container the following characters " + Strings.INVALID_FILENAME_CHARS);
        }
    }
}
