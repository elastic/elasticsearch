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

import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.index.mapper.object.RootObjectMapper;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * Wrapper around everything that defines a mapping, without references to
 * utility classes like MapperService, ...
 */
public final class Mapping implements ToXContent {

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

    final RootObjectMapper root;
    final RootMapper[] rootMappers;
    final RootMapper[] rootMappersNotIncludedInObject;
    final ImmutableMap<Class<? extends RootMapper>, RootMapper> rootMappersMap;
    final SourceTransform[] sourceTransforms;
    volatile ImmutableMap<String, Object> meta;

    public Mapping(RootObjectMapper rootObjectMapper, RootMapper[] rootMappers, SourceTransform[] sourceTransforms, ImmutableMap<String, Object> meta) {
        this.root = rootObjectMapper;
        this.rootMappers = rootMappers;
        List<RootMapper> rootMappersNotIncludedInObject = new ArrayList<>();
        ImmutableMap.Builder<Class<? extends RootMapper>, RootMapper> builder = ImmutableMap.builder();
        for (RootMapper rootMapper : rootMappers) {
            if (rootMapper.includeInObject()) {
                root.putMapper(rootMapper);
            } else {
                rootMappersNotIncludedInObject.add(rootMapper);
            }
            builder.put(rootMapper.getClass(), rootMapper);
        }
        this.rootMappersNotIncludedInObject = rootMappersNotIncludedInObject.toArray(new RootMapper[rootMappersNotIncludedInObject.size()]);
        this.rootMappersMap = builder.build();
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
        return new Mapping((RootObjectMapper) rootObjectMapper, rootMappers, sourceTransforms, meta);
    }

    /** Get the root mapper with the given class. */
    @SuppressWarnings("unchecked")
    public <T extends RootMapper> T rootMapper(Class<T> clazz) {
        return (T) rootMappersMap.get(clazz);
    }

    /** @see DocumentMapper#merge(Mapping, boolean) */
    public void merge(Mapping mergeWith, MergeResult mergeResult) {
        assert rootMappers.length == mergeWith.rootMappers.length;

        root.merge(mergeWith.root, mergeResult);
        for (RootMapper rootMapper : rootMappers) {
            // root mappers included in root object will get merge in the rootObjectMapper
            if (rootMapper.includeInObject()) {
                continue;
            }
            RootMapper mergeWithRootMapper = mergeWith.rootMapper(rootMapper.getClass());
            if (mergeWithRootMapper != null) {
                rootMapper.merge(mergeWithRootMapper, mergeResult);
            }
        }

        if (mergeResult.simulate() == false) {
            // let the merge with attributes to override the attributes
            meta = mergeWith.meta;
        }
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
                return builder;
            }
            // no need to pass here id and boost, since they are added to the root object mapper
            // in the constructor
        }, rootMappersNotIncludedInObject);
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
