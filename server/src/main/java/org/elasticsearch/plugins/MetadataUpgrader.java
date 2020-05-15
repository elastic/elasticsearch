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

import org.elasticsearch.cluster.metadata.IndexTemplateMetadata;
import org.elasticsearch.cluster.metadata.Metadata;

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.function.UnaryOperator;

/**
 * Upgrades {@link Metadata} on startup on behalf of installed {@link Plugin}s
 */
public class MetadataUpgrader {
    public final UnaryOperator<Map<String, IndexTemplateMetadata>> indexTemplateMetadataUpgraders;

    public MetadataUpgrader(Collection<UnaryOperator<Map<String, IndexTemplateMetadata>>> indexTemplateMetadataUpgraders) {
        this.indexTemplateMetadataUpgraders = templates -> {
            Map<String, IndexTemplateMetadata> upgradedTemplates = new HashMap<>(templates);
            for (UnaryOperator<Map<String, IndexTemplateMetadata>> upgrader : indexTemplateMetadataUpgraders) {
                upgradedTemplates = upgrader.apply(upgradedTemplates);
            }
            return upgradedTemplates;
        };
    }
}
