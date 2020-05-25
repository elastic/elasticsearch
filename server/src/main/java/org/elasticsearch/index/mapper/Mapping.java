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

import org.elasticsearch.Version;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.ToXContentFragment;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.Arrays;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Map;

import static java.util.Collections.emptyMap;
import static java.util.Collections.unmodifiableMap;

/**
 * Wrapper around everything that defines a mapping, without references to
 * utility classes like MapperService, ...
 */
public final class Mapping implements ToXContentFragment {

    final Version indexCreated;
    final RootObjectMapper root;
    final MetadataFieldMapper[] metadataMappers;
    final Map<Class<? extends MetadataFieldMapper>, MetadataFieldMapper> metadataMappersMap;
    final Map<String, Object> meta;

    public Mapping(Version indexCreated, RootObjectMapper rootObjectMapper,
                   MetadataFieldMapper[] metadataMappers, Map<String, Object> meta) {
        this.indexCreated = indexCreated;
        this.metadataMappers = metadataMappers;
        Map<Class<? extends MetadataFieldMapper>, MetadataFieldMapper> metadataMappersMap = new HashMap<>();
        for (MetadataFieldMapper metadataMapper : metadataMappers) {
            metadataMappersMap.put(metadataMapper.getClass(), metadataMapper);
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
        return new Mapping(indexCreated, (RootObjectMapper) rootObjectMapper, metadataMappers, meta);
    }

    /** Get the root mapper with the given class. */
    @SuppressWarnings("unchecked")
    public <T extends MetadataFieldMapper> T metadataMapper(Class<T> clazz) {
        return (T) metadataMappersMap.get(clazz);
    }

    /** @see DocumentMapper#merge(Mapping) */
    public Mapping merge(Mapping mergeWith) {
        RootObjectMapper mergedRoot = root.merge(mergeWith.root);
        Map<Class<? extends MetadataFieldMapper>, MetadataFieldMapper> mergedMetadataMappers = new HashMap<>(metadataMappersMap);
        for (MetadataFieldMapper metaMergeWith : mergeWith.metadataMappers) {
            MetadataFieldMapper mergeInto = mergedMetadataMappers.get(metaMergeWith.getClass());
            MetadataFieldMapper merged;
            if (mergeInto == null) {
                merged = metaMergeWith;
            } else {
                merged = (MetadataFieldMapper) mergeInto.merge(metaMergeWith);
            }
            mergedMetadataMappers.put(merged.getClass(), merged);
        }
        Map<String, Object> mergedMeta = mergeWith.meta == null ? meta : mergeWith.meta;
        return new Mapping(indexCreated, mergedRoot, mergedMetadataMappers.values().toArray(new MetadataFieldMapper[0]), mergedMeta);
    }

    /**
     * Recursively update sub field types.
     */
    public Mapping updateFieldType(Map<String, MappedFieldType> fullNameToFieldType) {
        MetadataFieldMapper[] updatedMeta = null;
        for (int i = 0; i < metadataMappers.length; ++i) {
            MetadataFieldMapper currentFieldMapper = metadataMappers[i];
            MetadataFieldMapper updatedFieldMapper = (MetadataFieldMapper) currentFieldMapper.updateFieldType(fullNameToFieldType);
            if (updatedFieldMapper != currentFieldMapper) {
                if (updatedMeta == null) {
                    updatedMeta = Arrays.copyOf(metadataMappers, metadataMappers.length);
                }
                updatedMeta[i] = updatedFieldMapper;
            }
        }
        RootObjectMapper updatedRoot = root.updateFieldType(fullNameToFieldType);
        if (updatedMeta == null && updatedRoot == root) {
            return this;
        }
        return new Mapping(indexCreated, updatedRoot, updatedMeta == null ? metadataMappers : updatedMeta, meta);
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        root.toXContent(builder, params, new ToXContent() {
            @Override
            public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
                if (meta != null) {
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

    @Override
    public String toString() {
        try {
            XContentBuilder builder = XContentFactory.jsonBuilder().startObject();
            toXContent(builder, new ToXContent.MapParams(emptyMap()));
            return Strings.toString(builder.endObject());
        } catch (IOException bogus) {
            throw new UncheckedIOException(bogus);
        }
    }
}
