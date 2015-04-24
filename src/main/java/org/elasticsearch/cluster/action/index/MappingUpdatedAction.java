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

package org.elasticsearch.cluster.action.index;

import org.elasticsearch.ElasticsearchIllegalArgumentException;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.admin.indices.mapping.put.PutMappingRequestBuilder;
import org.elasticsearch.action.admin.indices.mapping.put.PutMappingResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.client.IndicesAdminClient;
import org.elasticsearch.common.component.AbstractComponent;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.index.mapper.MapperService;
import org.elasticsearch.index.mapper.Mapping;
import org.elasticsearch.node.settings.NodeSettingsService;

import java.util.concurrent.TimeoutException;

/**
 * Called by shards in the cluster when their mapping was dynamically updated and it needs to be updated
 * in the cluster state meta data (and broadcast to all members).
 */
public class MappingUpdatedAction extends AbstractComponent {

    public static final String INDICES_MAPPING_DYNAMIC_TIMEOUT = "indices.mapping.dynamic_timeout";

    private IndicesAdminClient client;
    private volatile TimeValue dynamicMappingUpdateTimeout;

    class ApplySettings implements NodeSettingsService.Listener {
        @Override
        public void onRefreshSettings(Settings settings) {
            TimeValue current = MappingUpdatedAction.this.dynamicMappingUpdateTimeout;
            TimeValue newValue = settings.getAsTime(INDICES_MAPPING_DYNAMIC_TIMEOUT, current);
            if (!current.equals(newValue)) {
                logger.info("updating " + INDICES_MAPPING_DYNAMIC_TIMEOUT + " from [{}] to [{}]", current, newValue);
                MappingUpdatedAction.this.dynamicMappingUpdateTimeout = newValue;
            }
        }
    }

    @Inject
    public MappingUpdatedAction(Settings settings, NodeSettingsService nodeSettingsService) {
        super(settings);
        this.dynamicMappingUpdateTimeout = settings.getAsTime(INDICES_MAPPING_DYNAMIC_TIMEOUT, TimeValue.timeValueSeconds(30));
        nodeSettingsService.addListener(new ApplySettings());
    }

    public void setClient(Client client) {
        this.client = client.admin().indices();
    }

    private PutMappingRequestBuilder updateMappingRequest(String index, String type, Mapping mappingUpdate, final TimeValue timeout) {
        if (type.equals(MapperService.DEFAULT_MAPPING)) {
            throw new ElasticsearchIllegalArgumentException("_default_ mapping should not be updated");
        }
        return client.preparePutMapping(index).setType(type).setSource(mappingUpdate.toString())
            .setMasterNodeTimeout(timeout).setTimeout(timeout);
    }

    public void updateMappingOnMaster(String index, String type, Mapping mappingUpdate, final TimeValue timeout, final MappingUpdateListener listener) {
        final PutMappingRequestBuilder request = updateMappingRequest(index, type, mappingUpdate, timeout);
        if (listener == null) {
            request.execute();
        } else {
            final ActionListener<PutMappingResponse> actionListener = new ActionListener<PutMappingResponse>() {
                @Override
                public void onResponse(PutMappingResponse response) {
                    if (response.isAcknowledged()) {
                        listener.onMappingUpdate();
                    } else {
                        listener.onFailure(new TimeoutException("Failed to acknowledge the mapping response within [" + timeout + "]"));
                    }
                }

                @Override
                public void onFailure(Throwable e) {
                    listener.onFailure(e);
                }
            };
            request.execute(actionListener);
        }
    }

    public void updateMappingOnMasterAsynchronously(String index, String type, Mapping mappingUpdate) throws Throwable {
        updateMappingOnMaster(index, type, mappingUpdate, dynamicMappingUpdateTimeout, null);
    }

    /**
     * Same as {@link #updateMappingOnMasterSynchronously(String, String, String, Mapping, TimeValue)}
     * using the default timeout.
     */
    public void updateMappingOnMasterSynchronously(String index, String type, Mapping mappingUpdate) throws Throwable {
        updateMappingOnMasterSynchronously(index, type, mappingUpdate, dynamicMappingUpdateTimeout);
    }

    /**
     * Update mappings synchronously on the master node, waiting for at most
     * {@code timeout}. When this method returns successfully mappings have
     * been applied to the master node and propagated to data nodes.
     */
    public void updateMappingOnMasterSynchronously(String index, String type, Mapping mappingUpdate, TimeValue timeout) throws Throwable {
        if (updateMappingRequest(index, type, mappingUpdate, timeout).get().isAcknowledged() == false) {
            throw new TimeoutException("Failed to acknowledge mapping update within [" + timeout + "]");
        }
    }

    /**
     * A listener to be notified when the mappings were updated
     */
    public static interface MappingUpdateListener {

        void onMappingUpdate();

        void onFailure(Throwable t);
    }
}