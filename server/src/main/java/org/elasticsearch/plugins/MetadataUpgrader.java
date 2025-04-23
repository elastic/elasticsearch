/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.plugins;

import org.elasticsearch.cluster.metadata.IndexTemplateMetadata;
import org.elasticsearch.cluster.metadata.Metadata;

import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.UnaryOperator;

import static java.util.stream.Collectors.collectingAndThen;
import static java.util.stream.Collectors.groupingBy;
import static java.util.stream.Collectors.mapping;
import static java.util.stream.Collectors.toList;

/**
 * Upgrades {@link Metadata} on startup on behalf of installed {@link Plugin}s
 */
public class MetadataUpgrader {
    public final UnaryOperator<Map<String, IndexTemplateMetadata>> indexTemplateMetadataUpgraders;
    public final Map<String, UnaryOperator<Metadata.ProjectCustom>> customMetadataUpgraders;

    public MetadataUpgrader(
        Collection<UnaryOperator<Map<String, IndexTemplateMetadata>>> indexTemplateMetadataUpgraders,
        Collection<Map<String, UnaryOperator<Metadata.ProjectCustom>>> customMetadataUpgraders
    ) {
        this.indexTemplateMetadataUpgraders = templates -> {
            Map<String, IndexTemplateMetadata> upgradedTemplates = new HashMap<>(templates);
            for (UnaryOperator<Map<String, IndexTemplateMetadata>> upgrader : indexTemplateMetadataUpgraders) {
                upgradedTemplates = upgrader.apply(upgradedTemplates);
            }
            return upgradedTemplates;
        };
        this.customMetadataUpgraders = customMetadataUpgraders.stream()
            // Flatten the stream of maps into a stream of entries
            .flatMap(map -> map.entrySet().stream())
            .collect(
                groupingBy(
                    // Group by the type of custom metadata to be upgraded (the entry key)
                    Map.Entry::getKey,
                    // For each type, extract the operators (the entry values), collect to a list, and make an operator which combines them
                    collectingAndThen(mapping(Map.Entry::getValue, toList()), CombiningCustomUpgrader::new)
                )
            );
    }

    private record CombiningCustomUpgrader(List<UnaryOperator<Metadata.ProjectCustom>> upgraders)
        implements
            UnaryOperator<Metadata.ProjectCustom> {

        @Override
        public Metadata.ProjectCustom apply(Metadata.ProjectCustom custom) {
            Metadata.ProjectCustom upgraded = custom;
            for (UnaryOperator<Metadata.ProjectCustom> upgrader : upgraders) {
                upgraded = upgrader.apply(upgraded);
            }
            return upgraded;
        }
    }

}
