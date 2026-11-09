/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.mapper;

import org.elasticsearch.ElasticsearchGenerationException;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.compress.CompressedXContent;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.features.NodeFeature;
import org.elasticsearch.index.mapper.MapperService.MergeReason;
import org.elasticsearch.xcontent.ToXContent;
import org.elasticsearch.xcontent.ToXContentFragment;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentFactory;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.function.BiConsumer;
import java.util.function.Predicate;

/**
 * Mutable builder for a {@link Mapping}. Holds a {@link RootObjectMapper.Builder} and metadata field builders,
 * allowing merge operations to happen at the builder level before building the final immutable {@link Mapping}.
 */
public class MappingBuilder implements ToXContentFragment {

    private static final List<Params> PARAMS_TO_CHECK = List.of(
        ToXContent.EMPTY_PARAMS,
        new ToXContent.MapParams(Map.of("include_defaults", "true"))
    );

    private final RootObjectMapper.Builder rootBuilder;
    private final Map<String, MetadataFieldMapper.Builder> metadataBuilders;
    @Nullable
    private Map<String, Object> meta;
    private final boolean isStrictColumnar;

    public MappingBuilder(
        RootObjectMapper.Builder rootBuilder,
        Map<String, MetadataFieldMapper.Builder> metadataBuilders,
        @Nullable Map<String, Object> meta
    ) {
        this(rootBuilder, metadataBuilders, meta, false);
    }

    public MappingBuilder(
        RootObjectMapper.Builder rootBuilder,
        Map<String, MetadataFieldMapper.Builder> metadataBuilders,
        @Nullable Map<String, Object> meta,
        boolean isStrictColumnar
    ) {
        this.rootBuilder = rootBuilder;
        this.metadataBuilders = metadataBuilders;
        this.meta = meta;
        this.isStrictColumnar = isStrictColumnar;
    }

    /**
     * Creates a new {@link MappingBuilder} with the same root settings and metadata but no child
     * mappers in the root. Useful for applying field budget constraints without decomposing a built mapping.
     */
    MappingBuilder withoutMappers() {
        return new MappingBuilder(rootBuilder.newEmptyBuilder(), new LinkedHashMap<>(metadataBuilders), meta, isStrictColumnar);
    }

    public RootObjectMapper.Builder rootBuilder() {
        return rootBuilder;
    }

    public MetadataFieldMapper.Builder metadataBuilder(String name) {
        return metadataBuilders.get(name);
    }

    /**
     * Merges another {@link MappingBuilder} into this one, mutating this builder in place,
     * enforcing all limits in the provided {@link ParseFieldLimits}.
     */
    public void merge(
        MappingBuilder incoming,
        MergeReason reason,
        ParseFieldLimits fieldLimits,
        Predicate<NodeFeature> clusterSupportsFeature
    ) {
        mergeWith(
            incoming,
            reason,
            MapperMergeContext.root(
                isSourceSynthetic(),
                false,
                reason,
                fieldLimits,
                isStrictColumnar,
                isSourceColumnarStored(),
                clusterSupportsFeature
            )
        );
    }

    private void mergeWith(MappingBuilder incoming, MergeReason reason, MapperMergeContext mergeContext) {
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
        // Serializing consults state that is not available during recovery, such as the inference model registry
        boolean checkSerialization = reason != MergeReason.MAPPING_RECOVERY;
        List<String> serializedBeforeBuild = null;
        assert checkSerialization == false || (serializedBeforeBuild = serializationsAfterNormalizing(reason)) != null;
        RootObjectMapper root = rootBuilder.build(rootContext(reason));
        for (MetadataFieldMapper.Builder metadataBuilder : metadataBuilders.values()) {
            metadataBuilder.validateMapping(rootBuilder);
        }
        MetadataFieldMapper[] metadataMappers = metadataBuilders.values()
            .stream()
            .map(MetadataFieldMapper.Builder::build)
            .toArray(MetadataFieldMapper[]::new);
        Mapping mapping = new Mapping(root, metadataMappers, meta);
        assert checkSerialization == false || serializedBeforeBuild.isEmpty() || serializedBeforeBuild.equals(serializations())
            : "building changed the serialization of the builder: BEFORE "
                + serializedBeforeBuild.get(0)
                + " AFTER "
                + serializations().get(0);
        return mapping;
    }

    // Only assertions call this: the root builder normalizes the tree itself when it is built.
    // Returns nothing when serializing fails, as a mapping that building rejects can be impossible to serialize, and the
    // rejection is the error to report.
    private List<String> serializationsAfterNormalizing(MergeReason reason) {
        normalize(reason);
        try {
            return serializations();
        } catch (RuntimeException e) {
            return List.of();
        }
    }

    private List<String> serializations() {
        return PARAMS_TO_CHECK.stream().map(params -> serialize(this, params)).toList();
    }

    /**
     * Writes the mapping source this builder produces. A tree that has not been normalized yet is normalized first under
     * {@link MergeReason#MAPPING_UPDATE}, so a duplicate that only {@link MergeReason#INDEX_TEMPLATE} resolves throws a
     * conflict instead of being serialized as last-wins. Callers holding a template tree must {@link #normalize} or
     * {@link #build} it under its own reason first, after which this finds nothing left to normalize.
     */
    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        normalize(MergeReason.MAPPING_UPDATE);
        rootBuilder.toXContent(builder, params, (b, params1) -> {
            if (meta != null) {
                b.field("_meta", meta);
            }
            for (MetadataFieldMapper.Builder metadataBuilder : new TreeMap<>(metadataBuilders).values()) {
                metadataBuilder.toXContent(b, params1);
            }
            return b;
        });
        return builder;
    }

    public CompressedXContent toCompressedXContent() {
        try {
            return new CompressedXContent(this);
        } catch (Exception e) {
            throw new ElasticsearchGenerationException("failed to serialize source for type [" + rootBuilder.leafName() + "]", e);
        }
    }

    /**
     * Serializes with the given params, as a stand-alone object.
     */
    private static String serialize(ToXContentFragment fragment, Params params) {
        try (XContentBuilder builder = XContentFactory.jsonBuilder()) {
            builder.startObject();
            fragment.toXContent(builder, params);
            builder.endObject();
            return Strings.toString(builder);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    /**
     * Normalizes the whole builder tree
     *
     * @param reason the merge reason, used to configure the {@link MapperBuilderContext} for normalizing
     */
    public void normalize(MergeReason reason) {
        MapperBuilderContext rootContext = rootContext(reason);
        rootBuilder.normalize(rootContext);
        for (MetadataFieldMapper.Builder metadataBuilder : metadataBuilders.values()) {
            metadataBuilder.normalize(rootContext);
        }
    }

    /**
     * Normalizes the tree, then hands the consumer the builder of every field, metadata field and field alias with its full
     * path. Collected builders may be serialized individually as they have been normalized.
     *
     * @param reason   the merge reason, used to normalize
     * @param consumer receives each field builder with its full path
     */
    public void collectFieldBuilders(MergeReason reason, BiConsumer<String, Mapper.Builder> consumer) {
        normalize(reason);
        for (MetadataFieldMapper.Builder metadataBuilder : metadataBuilders.values()) {
            metadataBuilder.collectFieldBuilders(null, consumer);
        }
        rootBuilder.collectFieldBuilders(null, consumer);
    }

    private MapperBuilderContext rootContext(MergeReason reason) {
        return MapperBuilderContext.root(isSourceSynthetic(), isDataStream(), reason, isStrictColumnar, isSourceColumnarStored());
    }

    private boolean isSourceSynthetic() {
        MetadataFieldMapper.Builder builder = metadataBuilders.get(SourceFieldMapper.NAME);
        // columnar_stored pre-computes the synthetic source at indexing time, so mappers must
        // prepare the same fallback storage (doc values, stored fields) they would in synthetic mode.
        return builder instanceof SourceFieldMapper.Builder sfb && (sfb.isSynthetic() || sfb.isColumnarStored());
    }

    private boolean isSourceColumnarStored() {
        MetadataFieldMapper.Builder builder = metadataBuilders.get(SourceFieldMapper.NAME);
        return builder instanceof SourceFieldMapper.Builder sfb && sfb.isColumnarStored();
    }

    private boolean isDataStream() {
        MetadataFieldMapper.Builder builder = metadataBuilders.get(DataStreamTimestampFieldMapper.NAME);
        return builder instanceof DataStreamTimestampFieldMapper.Builder dsfb && dsfb.isEnabled();
    }
}
