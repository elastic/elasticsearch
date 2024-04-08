/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.index.mapper;

import org.elasticsearch.ElasticsearchGenerationException;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.compress.CompressedXContent;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.index.mapper.MapperService.MergeReason;
import org.elasticsearch.xcontent.ToXContentFragment;
import org.elasticsearch.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.Arrays;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Map;

/**
 * Wrapper around everything that defines a mapping, without references to
 * utility classes like MapperService, ...
 */
public final class Mapping implements ToXContentFragment {

    public static final Mapping EMPTY = new Mapping(
        new RootObjectMapper.Builder(MapperService.SINGLE_MAPPING_NAME, ObjectMapper.Defaults.SUBOBJECTS).build(
            MapperBuilderContext.root(false, false)
        ),
        new MetadataFieldMapper[0],
        null
    );

    private final RootObjectMapper root;
    private final Map<String, Object> meta;
    private final MetadataFieldMapper[] metadataMappers;
    private final Map<Class<? extends MetadataFieldMapper>, MetadataFieldMapper> metadataMappersMap;
    private final Map<String, MetadataFieldMapper> metadataMappersByName;

    // IntelliJ doesn't think that we need a rawtypes suppression here, but gradle fails to compile this file without it
    @SuppressWarnings({ "unchecked", "rawtypes" })
    public Mapping(RootObjectMapper rootObjectMapper, MetadataFieldMapper[] metadataMappers, Map<String, Object> meta) {
        this.metadataMappers = metadataMappers;
        Map.Entry<Class<? extends MetadataFieldMapper>, MetadataFieldMapper>[] metadataMappersMap = new Map.Entry[metadataMappers.length];
        Map.Entry<String, MetadataFieldMapper>[] metadataMappersByName = new Map.Entry[metadataMappers.length];
        for (int i = 0; i < metadataMappers.length; i++) {
            MetadataFieldMapper metadataMapper = metadataMappers[i];
            metadataMappersMap[i] = Map.entry(metadataMapper.getClass(), metadataMapper);
            metadataMappersByName[i] = Map.entry(metadataMapper.name(), metadataMapper);
        }
        this.root = rootObjectMapper;
        // keep root mappers sorted for consistent serialization
        Arrays.sort(metadataMappers, Comparator.comparing(Mapper::name));
        this.metadataMappersMap = Map.ofEntries(metadataMappersMap);
        this.metadataMappersByName = Map.ofEntries(metadataMappersByName);
        this.meta = meta;
    }

    /**
     * Outputs this mapping instance and returns it in {@link CompressedXContent} format
     * @return the {@link CompressedXContent} representation of this mapping instance
     */
    public CompressedXContent toCompressedXContent() {
        try {
            return new CompressedXContent(this);
        } catch (Exception e) {
            throw new ElasticsearchGenerationException("failed to serialize source for type [" + root.name() + "]", e);
        }
    }

    /**
     * Returns the root object for the current mapping
     */
    RootObjectMapper getRoot() {
        return root;
    }

    /**
     * Returns the meta section for the current mapping
     */
    public Map<String, Object> getMeta() {
        return meta;
    }

    MetadataFieldMapper[] getSortedMetadataMappers() {
        return metadataMappers;
    }

    Map<Class<? extends MetadataFieldMapper>, MetadataFieldMapper> getMetadataMappersMap() {
        return metadataMappersMap;
    }

    /** Get the metadata mapper with the given class. */
    @SuppressWarnings("unchecked")
    public <T extends MetadataFieldMapper> T getMetadataMapperByClass(Class<T> clazz) {
        return (T) metadataMappersMap.get(clazz);
    }

    MetadataFieldMapper getMetadataMapperByName(String mapperName) {
        return metadataMappersByName.get(mapperName);
    }

    void validate(MappingLookup mappers) {
        for (MetadataFieldMapper metadataFieldMapper : metadataMappers) {
            metadataFieldMapper.validate(mappers);
        }
        root.validate(mappers);
    }

    /**
     * Generate a mapping update for the given root object mapper.
     */
    Mapping mappingUpdate(RootObjectMapper rootObjectMapper) {
        return new Mapping(rootObjectMapper, metadataMappers, meta);
    }

    private boolean isSourceSynthetic() {
        SourceFieldMapper sfm = (SourceFieldMapper) metadataMappersByName.get(SourceFieldMapper.NAME);
        return sfm != null && sfm.isSynthetic();
    }

    public SourceLoader.SyntheticFieldLoader syntheticFieldLoader() {
        return root.syntheticFieldLoader(Arrays.stream(metadataMappers));
    }

    /**
     * Merges a new mapping into the existing one.
     *
     * @param mergeWith the new mapping to merge into this one.
     * @param reason the reason this merge was initiated.
     * @param newFieldsBudget how many new fields can be added during the merge process
     * @return the resulting merged mapping.
     */
    Mapping merge(Mapping mergeWith, MergeReason reason, long newFieldsBudget) {
        MapperMergeContext mergeContext = MapperMergeContext.root(isSourceSynthetic(), false, newFieldsBudget);
        RootObjectMapper mergedRoot = root.merge(mergeWith.root, reason, mergeContext);

        // When merging metadata fields as part of applying an index template, new field definitions
        // completely overwrite existing ones instead of being merged. This behavior matches how we
        // merge leaf fields in the 'properties' section of the mapping.
        Map<Class<? extends MetadataFieldMapper>, MetadataFieldMapper> mergedMetadataMappers = new HashMap<>(metadataMappersMap);
        for (MetadataFieldMapper metaMergeWith : mergeWith.metadataMappers) {
            MetadataFieldMapper mergeInto = mergedMetadataMappers.get(metaMergeWith.getClass());
            MetadataFieldMapper merged;
            if (mergeInto == null || reason == MergeReason.INDEX_TEMPLATE) {
                merged = metaMergeWith;
            } else {
                merged = (MetadataFieldMapper) mergeInto.merge(metaMergeWith, mergeContext);
            }
            mergedMetadataMappers.put(merged.getClass(), merged);
        }

        // If we are merging the _meta object as part of applying an index template, then the new object
        // is deep-merged into the existing one to allow individual keys to be added or overwritten. For
        // standard mapping updates, the new _meta object completely replaces the old one.
        Map<String, Object> mergedMeta;
        if (mergeWith.meta == null) {
            mergedMeta = meta;
        } else if (meta == null || reason != MergeReason.INDEX_TEMPLATE) {
            mergedMeta = mergeWith.meta;
        } else {
            mergedMeta = new HashMap<>(mergeWith.meta);
            XContentHelper.mergeDefaults(mergedMeta, meta);
        }

        return new Mapping(mergedRoot, mergedMetadataMappers.values().toArray(new MetadataFieldMapper[0]), mergedMeta);
    }

    /**
     * Returns a copy of this mapper that ensures that the number of fields isn't greater than the provided fields budget.
     * @param fieldsBudget the maximum number of fields this mapping may have
     */
    public Mapping withFieldsBudget(long fieldsBudget) {
        MapperMergeContext mergeContext = MapperMergeContext.root(isSourceSynthetic(), false, fieldsBudget);
        // get a copy of the root mapper, without any fields
        RootObjectMapper shallowRoot = root.withoutMappers();
        // calling merge on the shallow root to ensure we're only adding as many fields as allowed by the fields budget
        return new Mapping(shallowRoot.merge(root, MergeReason.MAPPING_RECOVERY, mergeContext), metadataMappers, meta);
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        root.toXContent(builder, params, (b, params1) -> {
            if (meta != null) {
                b.field("_meta", meta);
            }
            for (Mapper mapper : metadataMappers) {
                mapper.toXContent(b, params1);
            }
            return b;
        });
        return builder;
    }

    @Override
    public String toString() {
        return Strings.toString(this);
    }
}
