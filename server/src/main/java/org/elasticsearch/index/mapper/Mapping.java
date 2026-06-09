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
import org.elasticsearch.core.Nullable;
import org.elasticsearch.search.lookup.SourceFilter;
import org.elasticsearch.xcontent.ToXContentFragment;
import org.elasticsearch.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.Arrays;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Map;
import java.util.stream.Stream;

/**
 * Wrapper around everything that defines a mapping, without references to
 * utility classes like MapperService, ...
 */
public final class Mapping implements ToXContentFragment {

    public static final Mapping EMPTY = new Mapping(
        new RootObjectMapper.Builder(MapperService.SINGLE_MAPPING_NAME).build(MapperBuilderContext.root(false, false)),
        new MetadataFieldMapper[0],
        null
    );

    /**
     * Holder class for lazy initialization of EMPTY_COMPRESSED. Creating a CompressedXContent calls
     * MessageDigests.sha256() which resets the thread-local SHA-256 digest, so we must not do this
     * during Mapping class loading (which can happen at unpredictable times via XContentHelper.parseToType).
     */
    private static class EmptyCompressedHolder {
        static final CompressedXContent INSTANCE = EMPTY.toCompressedXContent();
    }

    public static CompressedXContent emptyCompressed() {
        return EmptyCompressedHolder.INSTANCE;
    }

    private final RootObjectMapper root;
    private final Map<String, Object> meta;
    private final MetadataFieldMapper[] metadataMappers;
    private final Map<Class<? extends MetadataFieldMapper>, MetadataFieldMapper> metadataMappersMap;
    private final Map<String, MetadataFieldMapper> metadataMappersByName;

    private final FieldNamesFieldMapper fieldNamesFieldMapper; // cached from metadataMappersByClass

    // this allows the document parser (for example) to find the leaf mapper for a field with a single map lookup,
    // rather than checking two maps (with the first check usually being a miss)
    private final Map<String, Mapper> mergedRootAndMetadataMappers;

    // IntelliJ doesn't think that we need a rawtypes suppression here, but gradle fails to compile this file without it
    @SuppressWarnings({ "unchecked", "rawtypes" })
    public Mapping(RootObjectMapper rootObjectMapper, MetadataFieldMapper[] metadataMappers, Map<String, Object> meta) {
        this.metadataMappers = metadataMappers;
        Map.Entry<Class<? extends MetadataFieldMapper>, MetadataFieldMapper>[] metadataMappersMap = new Map.Entry[metadataMappers.length];
        Map.Entry<String, MetadataFieldMapper>[] metadataMappersByName = new Map.Entry[metadataMappers.length];
        for (int i = 0; i < metadataMappers.length; i++) {
            MetadataFieldMapper metadataMapper = metadataMappers[i];
            metadataMappersMap[i] = Map.entry(metadataMapper.getClass(), metadataMapper);
            metadataMappersByName[i] = Map.entry(metadataMapper.fullPath(), metadataMapper);
        }
        this.root = rootObjectMapper;
        // keep root mappers sorted for consistent serialization
        Arrays.sort(metadataMappers, Comparator.comparing(Mapper::fullPath));
        this.metadataMappersMap = Map.ofEntries(metadataMappersMap);
        this.metadataMappersByName = Map.ofEntries(metadataMappersByName);
        this.meta = meta;

        // cache the field names field mapper
        this.fieldNamesFieldMapper = (FieldNamesFieldMapper) this.metadataMappersByName.get(FieldNamesFieldMapper.NAME);

        // squash together the root object mappers, overriding them with the metadataMappers
        var mappers = new HashMap<String, Mapper>();
        mappers.putAll(rootObjectMapper.getMappers());
        mappers.putAll(this.metadataMappersByName);
        this.mergedRootAndMetadataMappers = Map.copyOf(mappers);
    }

    /**
     * Outputs this mapping instance and returns it in {@link CompressedXContent} format
     * @return the {@link CompressedXContent} representation of this mapping instance
     */
    public CompressedXContent toCompressedXContent() {
        try {
            return new CompressedXContent(this);
        } catch (Exception e) {
            throw new ElasticsearchGenerationException("failed to serialize source for type [" + root.fullPath() + "]", e);
        }
    }

    /**
     * Returns the root object for the current mapping
     */
    public RootObjectMapper getRoot() {
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

    public MetadataFieldMapper getMetadataMapperByName(String mapperName) {
        return metadataMappersByName.get(mapperName);
    }

    public Mapper findMetadataOrRootMapper(String mapperName) {
        return mergedRootAndMetadataMappers.get(mapperName);
    }

    void validate(MappingLookup mappers) {
        for (MetadataFieldMapper metadataFieldMapper : metadataMappers) {
            metadataFieldMapper.validate(mappers);
        }
        root.validate(mappers);
    }

    /**
     * Returns a {@link SourceLoader.SyntheticVectorsLoader} that loads synthetic vector values
     * from a source document, optionally applying a {@link SourceFilter}.
     * <p>
     * The {@code filter}, if provided, can be used to limit which fields from the mapping
     * are considered when computing synthetic vectors. This allows for performance
     * optimizations or targeted vector extraction.
     * </p>
     *
     * @param filter an optional {@link SourceFilter} to restrict the fields considered during loading;
     *               may be {@code null} to indicate no filtering
     * @return a {@link SourceLoader.SyntheticVectorsLoader} for extracting synthetic vectors,
     *         potentially using the provided filter
     */
    public SourceLoader.SyntheticVectorsLoader syntheticVectorsLoader(@Nullable SourceFilter filter) {
        return root.syntheticVectorsLoader(filter);
    }

    public SourceLoader.SyntheticFieldLoader syntheticFieldLoader(@Nullable SourceFilter filter) {
        var mappers = Stream.concat(Stream.of(metadataMappers), root.mappers.values().stream()).toList();
        return root.syntheticFieldLoader(filter, mappers, false);
    }

    public IgnoredSourceFieldMapper.IgnoredSourceFormat ignoredSourceFormat() {
        IgnoredSourceFieldMapper isfm = (IgnoredSourceFieldMapper) metadataMappersByName.get(IgnoredSourceFieldMapper.NAME);
        if (isfm == null) {
            return IgnoredSourceFieldMapper.IgnoredSourceFormat.NO_IGNORED_SOURCE;
        }
        return isfm.ignoredSourceFormat();
    }

    public FieldNamesFieldMapper fieldNamesFieldMapper() {
        return fieldNamesFieldMapper;
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
