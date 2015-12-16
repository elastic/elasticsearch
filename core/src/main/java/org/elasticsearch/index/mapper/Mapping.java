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

package org.elasticsearch.index.mapper;

import com.google.common.collect.ImmutableMap;

import org.elasticsearch.Version;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.index.mapper.object.RootObjectMapper;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

/**
 * Wrapper around everything that defines a mapping, without references to
 * utility classes like MapperService, ...
 */
public final class Mapping implements ToXContent {

    // Set of fields that were included into the root object mapper before 2.0
    public static final Set<String> LEGACY_INCLUDE_IN_OBJECT = Collections.unmodifiableSet(new HashSet<>(
            Arrays.asList("_all", "_id", "_parent", "_routing", "_timestamp", "_ttl")));

    /**
     * Transformations to be applied to the source before indexing and/or after loading.
     */
    public interface SourceTransform extends ToXContent {
        /**
         * Transform the source when it is expressed as a map.  This is public so it can be transformed the source is loaded.
         * @param sourceAsMap source to transform.  This may be mutated by the script.
         * @return transformed version of transformMe.  This may actually be the same object as sourceAsMap
         */
        Map<String, Object> transformSourceAsMap(Map<String, Object> sourceAsMap);
    }

    final Version indexCreated;
    final RootObjectMapper root;
    final MetadataFieldMapper[] metadataMappers;
    final ImmutableMap<Class<? extends MetadataFieldMapper>, MetadataFieldMapper> metadataMappersMap;
    final SourceTransform[] sourceTransforms;
    final ImmutableMap<String, Object> meta;

    public Mapping(Version indexCreated, RootObjectMapper rootObjectMapper, MetadataFieldMapper[] metadataMappers, SourceTransform[] sourceTransforms, ImmutableMap<String, Object> meta) {
        this.indexCreated = indexCreated;
        this.metadataMappers = metadataMappers;
        ImmutableMap.Builder<Class<? extends MetadataFieldMapper>, MetadataFieldMapper> builder = ImmutableMap.builder();
        for (MetadataFieldMapper metadataMapper : metadataMappers) {
            if (indexCreated.before(Version.V_2_0_0_beta1) && LEGACY_INCLUDE_IN_OBJECT.contains(metadataMapper.name())) {
                rootObjectMapper = rootObjectMapper.copyAndPutMapper(metadataMapper);
            }
            builder.put(metadataMapper.getClass(), metadataMapper);
        }
        this.root = rootObjectMapper;
        // keep root mappers sorted for consistent serialization
        Arrays.sort(metadataMappers, new Comparator<Mapper>() {
            @Override
            public int compare(Mapper o1, Mapper o2) {
                return o1.name().compareTo(o2.name());
            }
        });
        this.metadataMappersMap = builder.build();
        this.sourceTransforms = sourceTransforms;
        this.meta = meta;
    }

    /** Return the root object mapper. */
    public RootObjectMapper root() {
        return root;
    }

    /**
     * Generate a mapping update for the given root object mapper.
     */
    public Mapping mappingUpdate(Mapper rootObjectMapper) {
        return new Mapping(indexCreated, (RootObjectMapper) rootObjectMapper, metadataMappers, sourceTransforms, meta);
    }

    /** Get the root mapper with the given class. */
    @SuppressWarnings("unchecked")
    public <T extends MetadataFieldMapper> T metadataMapper(Class<T> clazz) {
        return (T) metadataMappersMap.get(clazz);
    }

    /** @see DocumentMapper#merge(Mapping, boolean) */
    public Mapping merge(Mapping mergeWith, boolean updateAllTypes) {
        RootObjectMapper mergedRoot = root.merge(mergeWith.root, updateAllTypes);
        Map<Class<? extends MetadataFieldMapper>, MetadataFieldMapper> mergedMetaDataMappers = new HashMap<>(metadataMappersMap);
        for (MetadataFieldMapper metaMergeWith : mergeWith.metadataMappers) {
            MetadataFieldMapper mergeInto = mergedMetaDataMappers.get(metaMergeWith.getClass());
            MetadataFieldMapper merged;
            if (mergeInto == null) {
                merged = metaMergeWith;
            } else {
                merged = mergeInto.merge(metaMergeWith, updateAllTypes);
            }
            mergedMetaDataMappers.put(merged.getClass(), merged);
        }
        return new Mapping(indexCreated, mergedRoot, mergedMetaDataMappers.values().toArray(new MetadataFieldMapper[0]), sourceTransforms, mergeWith.meta);
    }

    /**
     * Recursively update sub field types.
     */
    public Mapping updateFieldType(Map<String, MappedFieldType> fullNameToFieldType) {
        final MetadataFieldMapper[] updatedMeta = Arrays.copyOf(metadataMappers, metadataMappers.length);
        for (int i = 0; i < updatedMeta.length; ++i) {
            updatedMeta[i] = (MetadataFieldMapper) updatedMeta[i].updateFieldType(fullNameToFieldType);
        }
        RootObjectMapper updatedRoot = root.updateFieldType(fullNameToFieldType);
        return new Mapping(indexCreated, updatedRoot, updatedMeta, sourceTransforms, meta);
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        root.toXContent(builder, params, new ToXContent() {
            @Override
            public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
                if (sourceTransforms.length > 0) {
                    if (sourceTransforms.length == 1) {
                        builder.field("transform");
                        sourceTransforms[0].toXContent(builder, params);
                    } else {
                        builder.startArray("transform");
                        for (SourceTransform transform: sourceTransforms) {
                            transform.toXContent(builder, params);
                        }
                        builder.endArray();
                    }
                }

                if (meta != null && !meta.isEmpty()) {
                    builder.field("_meta", meta);
                }
                for (Mapper mapper : metadataMappers) {
                    mapper.toXContent(builder, params);
                }
                return builder;
            }
        });
        return builder;
    }

    /** Serialize to a {@link BytesReference}. */
    public BytesReference toBytes() {
        try {
            XContentBuilder builder = XContentFactory.jsonBuilder().startObject();
            toXContent(builder, new ToXContent.MapParams(ImmutableMap.<String, String>of()));
            return builder.endObject().bytes();
        } catch (IOException bogus) {
            throw new AssertionError(bogus);
        }
    }

    @Override
    public String toString() {
        try {
            XContentBuilder builder = XContentFactory.jsonBuilder().startObject();
            toXContent(builder, new ToXContent.MapParams(ImmutableMap.<String, String>of()));
            return builder.endObject().string();
        } catch (IOException bogus) {
            throw new AssertionError(bogus);
        }
    }
}
