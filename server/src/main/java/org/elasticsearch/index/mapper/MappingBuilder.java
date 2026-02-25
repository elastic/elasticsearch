/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.mapper;

import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.index.mapper.MapperService.MergeReason;

import java.util.LinkedHashMap;
import java.util.Map;

/**
 * Mutable builder for a {@link Mapping}. Holds a {@link RootObjectMapper.Builder} and metadata field builders,
 * allowing merge operations to happen at the builder level before building the final immutable {@link Mapping}.
 */
public class MappingBuilder {

    private final RootObjectMapper.Builder rootBuilder;
    private final Map<String, MetadataFieldMapper.Builder> metadataBuilders;
    @Nullable
    private Map<String, Object> meta;

    public MappingBuilder(
        RootObjectMapper.Builder rootBuilder,
        Map<String, MetadataFieldMapper.Builder> metadataBuilders,
        @Nullable Map<String, Object> meta
    ) {
        this.rootBuilder = rootBuilder;
        this.metadataBuilders = metadataBuilders;
        this.meta = meta;
    }

    /**
     * Creates a new {@link MappingBuilder} with the same root settings and metadata but no child
     * mappers in the root. Useful for applying field budget constraints without decomposing a built mapping.
     */
    MappingBuilder withoutMappers() {
        return new MappingBuilder(rootBuilder.newEmptyBuilder(), new LinkedHashMap<>(metadataBuilders), meta);
    }

    public RootObjectMapper.Builder rootBuilder() {
        return rootBuilder;
    }

    /**
     * Merges another {@link MappingBuilder} into this one, mutating this builder in place.
     *
     * @param incoming the incoming mapping builder to merge
     * @param reason the reason for the merge
     * @param newFieldsBudget how many new fields may be added during the merge
     */
    public void merge(MappingBuilder incoming, MergeReason reason, long newFieldsBudget) {
        MapperMergeContext mergeContext = MapperMergeContext.root(isSourceSynthetic(), false, reason, newFieldsBudget);

        // Merge root object builders
        MapperMergeContext objectMergeContext = mergeContext.createChildContext(null, rootBuilder.dynamic);
        rootBuilder.merge(incoming.rootBuilder, objectMergeContext, rootBuilder.leafName());

        // Merge metadata field builders, skipping incoming builders that weren't explicitly set
        // (if a metadata field wasn't explicitly set in the incoming mapping, there's nothing to merge)
        for (var entry : incoming.metadataBuilders.entrySet()) {
            MetadataFieldMapper.Builder incomingBuilder = entry.getValue();
            MetadataFieldMapper.Builder existingBuilder = this.metadataBuilders.get(entry.getKey());
            if (existingBuilder == null || reason == MergeReason.INDEX_TEMPLATE) {
                this.metadataBuilders.put(entry.getKey(), incomingBuilder);
            } else if (incomingBuilder.isConfigured()) {
                this.metadataBuilders.put(
                    entry.getKey(),
                    (MetadataFieldMapper.Builder) existingBuilder.mergeWith(incomingBuilder, mergeContext)
                );
            }
        }

        // Merge _meta: for INDEX_TEMPLATE, deep-merge incoming over existing.
        // For other reasons, incoming replaces existing entirely.
        if (incoming.meta != null) {
            if (meta == null || reason != MergeReason.INDEX_TEMPLATE) {
                meta = incoming.meta;
            } else {
                Map<String, Object> existingMeta = meta;
                meta = new LinkedHashMap<>(incoming.meta);
                XContentHelper.mergeDefaults(meta, existingMeta);
            }
        }
    }

    /**
     * Builds the final immutable {@link Mapping} from the current builder state.
     *
     * @param reason the merge reason, used to configure the {@link MapperBuilderContext} for building
     * @return the built {@link Mapping}
     */
    public Mapping build(MergeReason reason) {
        MapperBuilderContext rootContext = MapperBuilderContext.root(isSourceSynthetic(), isDataStream(), reason);
        RootObjectMapper root = rootBuilder.build(rootContext);
        MetadataFieldMapper[] metadataMappers = metadataBuilders.values()
            .stream()
            .map(MetadataFieldMapper.Builder::build)
            .toArray(MetadataFieldMapper[]::new);
        return new Mapping(root, metadataMappers, meta);
    }

    private boolean isSourceSynthetic() {
        MetadataFieldMapper.Builder builder = metadataBuilders.get(SourceFieldMapper.NAME);
        return builder instanceof SourceFieldMapper.Builder sfb && sfb.isSynthetic();
    }

    private boolean isDataStream() {
        MetadataFieldMapper.Builder builder = metadataBuilders.get(DataStreamTimestampFieldMapper.NAME);
        return builder instanceof DataStreamTimestampFieldMapper.Builder dsfb && dsfb.isEnabled();
    }
}
