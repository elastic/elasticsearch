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
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.ToXContentFragment;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.index.mapper.MapperService.MergeReason;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.Arrays;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Map;

import static java.util.Collections.unmodifiableMap;

/**
 * Wrapper around everything that defines a mapping, without references to
 * utility classes like MapperService, ...
 */
public final class Mapping implements ToXContentFragment {

    public static final Mapping EMPTY = new Mapping(
        new RootObjectMapper.Builder("_doc").build(new ContentPath()), new MetadataFieldMapper[0], null);

    private final RootObjectMapper root;
    private final Map<String, Object> meta;
    private final MetadataFieldMapper[] metadataMappers;
    private final Map<Class<? extends MetadataFieldMapper>, MetadataFieldMapper> metadataMappersMap;
    private final Map<String, MetadataFieldMapper> metadataMappersByName;

    public Mapping(RootObjectMapper rootObjectMapper, MetadataFieldMapper[] metadataMappers, Map<String, Object> meta) {
        this.metadataMappers = metadataMappers;
        Map<Class<? extends MetadataFieldMapper>, MetadataFieldMapper> metadataMappersMap = new HashMap<>();
        Map<String, MetadataFieldMapper> metadataMappersByName = new HashMap<>();
        for (MetadataFieldMapper metadataMapper : metadataMappers) {
            metadataMappersMap.put(metadataMapper.getClass(), metadataMapper);
            metadataMappersByName.put(metadataMapper.name(), metadataMapper);
        }
        this.root = rootObjectMapper;
        // keep root mappers sorted for consistent serialization
        Arrays.sort(metadataMappers, new Comparator<Mapper>() {
            @Override
            public int compare(Mapper o1, Mapper o2) {
                return o1.name().compareTo(o2.name());
            }
        });
        this.metadataMappersMap = unmodifiableMap(metadataMappersMap);
        this.metadataMappersByName = unmodifiableMap(metadataMappersByName);
        this.meta = meta;

    }

    /**
     * Outputs this mapping instance and returns it in {@link CompressedXContent} format
     * @return the {@link CompressedXContent} representation of this mapping instance
     */
    public CompressedXContent toCompressedXContent() {
        try {
            return new CompressedXContent(this, XContentType.JSON, ToXContent.EMPTY_PARAMS);
        } catch (Exception e) {
            throw new ElasticsearchGenerationException("failed to serialize source for type [" + root.name() + "]", e);
        }
    }

    public String type() {
        return root.name();
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

    /**
     * Merges a new mapping into the existing one.
     *
     * @param mergeWith the new mapping to merge into this one.
     * @param reason    the reason this merge was initiated.
     * @return the resulting merged mapping.
     */
    public Mapping merge(Mapping mergeWith, MergeReason reason) {
        RootObjectMapper mergedRoot = root.merge(mergeWith.root, reason);

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
                merged = (MetadataFieldMapper) mergeInto.merge(metaMergeWith);
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
        try {
            XContentBuilder builder = XContentFactory.jsonBuilder().startObject();
            toXContent(builder, ToXContent.EMPTY_PARAMS);
            return Strings.toString(builder.endObject());
        } catch (IOException bogus) {
            throw new UncheckedIOException(bogus);
        }
    }
}
