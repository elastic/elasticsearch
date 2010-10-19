/*
 * Licensed to Elastic Search and Shay Banon under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. Elastic Search licenses this
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

package org.elasticsearch.cluster.metadata;

import org.elasticsearch.ElasticSearchIllegalArgumentException;
import org.elasticsearch.cluster.ClusterService;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.ProcessedClusterStateUpdateTask;
import org.elasticsearch.cluster.routing.RoutingTable;
import org.elasticsearch.common.component.AbstractComponent;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.common.settings.Settings;

import java.util.Map;

import static org.elasticsearch.cluster.routing.RoutingTable.*;

/**
 * @author kimchy (shay.banon)
 */
public class MetaDataUpdateSettingsService extends AbstractComponent {

    private final ClusterService clusterService;

    @Inject public MetaDataUpdateSettingsService(Settings settings, ClusterService clusterService) {
        super(settings);
        this.clusterService = clusterService;
    }

    public void updateSettings(final Settings pSettings, final String[] indices, final Listener listener) {
        ImmutableSettings.Builder updatedSettingsBuilder = ImmutableSettings.settingsBuilder();
        for (Map.Entry<String, String> entry : pSettings.getAsMap().entrySet()) {
            if (!entry.getKey().startsWith("index.")) {
                updatedSettingsBuilder.put("index." + entry.getKey(), entry.getValue());
            } else {
                updatedSettingsBuilder.put(entry.getKey(), entry.getValue());
            }
        }
        final Settings settings = updatedSettingsBuilder.build();
        clusterService.submitStateUpdateTask("update-settings", new ProcessedClusterStateUpdateTask() {
            @Override public ClusterState execute(ClusterState currentState) {
                try {
                    boolean changed = false;
                    String[] actualIndices = currentState.metaData().concreteIndices(indices);
                    RoutingTable.Builder routingTableBuilder = newRoutingTableBuilder().routingTable(currentState.routingTable());
                    MetaData.Builder metaDataBuilder = MetaData.newMetaDataBuilder().metaData(currentState.metaData());

                    int updatedNumberOfReplicas = settings.getAsInt(IndexMetaData.SETTING_NUMBER_OF_REPLICAS, -1);
                    if (updatedNumberOfReplicas != -1) {
                        routingTableBuilder.updateNumberOfReplicas(updatedNumberOfReplicas, actualIndices);
                        metaDataBuilder.updateNumberOfReplicas(updatedNumberOfReplicas, actualIndices);
                        changed = true;
                    }

                    if (!changed) {
                        listener.onFailure(new ElasticSearchIllegalArgumentException("No settings applied"));
                        return currentState;
                    }

                    logger.info("Updating number_of_replicas to [{}] for indices {}", updatedNumberOfReplicas, actualIndices);

                    return ClusterState.builder().state(currentState).metaData(metaDataBuilder).routingTable(routingTableBuilder).build();
                } catch (Exception e) {
                    listener.onFailure(e);
                    return currentState;
                }
            }

            @Override public void clusterStateProcessed(ClusterState clusterState) {
                listener.onSuccess();
            }
        });
    }

    public static interface Listener {
        void onSuccess();

        void onFailure(Throwable t);
    }
}
