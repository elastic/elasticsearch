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

package org.elasticsearch.plugins;

import org.elasticsearch.cluster.metadata.IndexTemplateMetaData;
import org.elasticsearch.cluster.metadata.MetaData;

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.function.UnaryOperator;

/**
 * Upgrades {@link MetaData} on startup on behalf of installed {@link Plugin}s
 */
public class MetaDataUpgrader {
    public final UnaryOperator<Map<String, MetaData.Custom>> customMetaDataUpgraders;

    public final UnaryOperator<Map<String, IndexTemplateMetaData>> indexTemplateMetaDataUpgraders;

    public MetaDataUpgrader(Collection<UnaryOperator<Map<String, MetaData.Custom>>> customMetaDataUpgraders,
                            Collection<UnaryOperator<Map<String, IndexTemplateMetaData>>> indexTemplateMetaDataUpgraders) {
        this.customMetaDataUpgraders = customs -> {
            Map<String, MetaData.Custom> upgradedCustoms = new HashMap<>(customs);
            for (UnaryOperator<Map<String, MetaData.Custom>> customMetaDataUpgrader : customMetaDataUpgraders) {
                upgradedCustoms = customMetaDataUpgrader.apply(upgradedCustoms);
            }
            return upgradedCustoms;
        };

        this.indexTemplateMetaDataUpgraders = templates -> {
            Map<String, IndexTemplateMetaData> upgradedTemplates = new HashMap<>(templates);
            for (UnaryOperator<Map<String, IndexTemplateMetaData>> upgrader : indexTemplateMetaDataUpgraders) {
                upgradedTemplates = upgrader.apply(upgradedTemplates);
            }
            return upgradedTemplates;
        };
    }
}
