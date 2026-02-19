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

import java.util.HashMap;
import java.util.Map;

/**
 * Mutable builder for a {@link Mapping}. Holds a {@link RootObjectMapper.Builder} and metadata field mappers,
 * allowing merge operations to happen at the builder level before building the final immutable {@link Mapping}.
 * <p>
 * The intent is that incoming mappings (from parsing or from existing {@link Mapping} objects) are converted
 * into {@link MappingBuilder}s, merged together, and only built into concrete {@link Mapping} objects at the
 * very end. This avoids creating transient intermediate {@link Mapper} objects during merges.
 */
public class MappingBuilder {

    private final RootObjectMapper.Builder rootBuilder;
    private final Map<Class<? extends MetadataFieldMapper>, MetadataFieldMapper> metadataMappers;
    @Nullable
    private Map<String, Object> meta;

    public MappingBuilder(
        RootObjectMapper.Builder rootBuilder,
        Map<Class<? extends MetadataFieldMapper>, MetadataFieldMapper> metadataMappers,
        @Nullable Map<String, Object> meta
    ) {
        this.rootBuilder = rootBuilder;
        this.metadataMappers = metadataMappers;
        this.meta = meta;
    }

    /**
     * Creates a {@link MappingBuilder} from an existing {@link Mapping}, converting the root mapper
     * and all its children into builder form.
     */
    public static MappingBuilder fromMapping(Mapping mapping) {
        return new MappingBuilder(mapping.getRoot().toBuilder(), new HashMap<>(mapping.getMetadataMappersMap()), mapping.getMeta());
    }

    /**
     * Creates a {@link MappingBuilder} from an existing {@link Mapping} but without any child mappers
     * in the root. Used to enforce field budget limits by merging fields one-by-one into an
     * otherwise empty root.
     */
    static MappingBuilder fromMappingWithoutMappers(Mapping mapping) {
        return new MappingBuilder(
            mapping.getRoot().withoutMappers().toBuilder(),
            new HashMap<>(mapping.getMetadataMappersMap()),
            mapping.getMeta()
        );
    }

    /**
     * Creates a new {@link MappingBuilder} with the same root settings and metadata but no child
     * mappers in the root. Useful for applying field budget constraints without decomposing a built mapping.
     */
    MappingBuilder withoutMappers() {
        return new MappingBuilder(rootBuilder.newEmptyBuilder(), new HashMap<>(metadataMappers), meta);
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

        // Merge metadata fields: for INDEX_TEMPLATE incoming wins, otherwise merge
        for (var entry : incoming.metadataMappers.entrySet()) {
            MetadataFieldMapper existing = this.metadataMappers.get(entry.getKey());
            MetadataFieldMapper merged;
            if (existing == null || reason == MergeReason.INDEX_TEMPLATE) {
                merged = entry.getValue();
            } else {
                FieldMapper.Builder existingBuilder = existing.getMergeBuilder();
                FieldMapper.Builder incomingBuilder = entry.getValue().getMergeBuilder();
                if (existingBuilder != null && incomingBuilder != null) {
                    merged = (MetadataFieldMapper) existingBuilder.mergeWith(incomingBuilder, mergeContext)
                        .build(mergeContext.getMapperBuilderContext());
                } else {
                    merged = entry.getValue();
                }
            }
            this.metadataMappers.put(merged.getClass(), merged);
        }

        // Merge _meta: for INDEX_TEMPLATE, deep-merge incoming over existing.
        // For other reasons, incoming replaces existing entirely.
        if (incoming.meta != null) {
            if (meta == null || reason != MergeReason.INDEX_TEMPLATE) {
                meta = incoming.meta;
            } else {
                Map<String, Object> existingMeta = meta;
                meta = new HashMap<>(incoming.meta);
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
        return new Mapping(root, metadataMappers.values().toArray(new MetadataFieldMapper[0]), meta);
    }

    private boolean isSourceSynthetic() {
        for (MetadataFieldMapper mapper : metadataMappers.values()) {
            if (mapper instanceof SourceFieldMapper sfm) {
                return sfm.isSynthetic();
            }
        }
        return false;
    }

    private boolean isDataStream() {
        for (MetadataFieldMapper mapper : metadataMappers.values()) {
            if (mapper instanceof DataStreamTimestampFieldMapper dsfm) {
                return dsfm.isEnabled();
            }
        }
        return false;
    }
}
