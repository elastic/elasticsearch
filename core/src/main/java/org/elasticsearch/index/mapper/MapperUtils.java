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

import org.elasticsearch.index.mapper.object.ObjectMapper;
import org.elasticsearch.index.mapper.object.RootObjectMapper;

import java.util.Collection;

public enum MapperUtils {
    ;

    private static MergeResult newStrictMergeResult() {
        return new MergeResult(false, false) {

            @Override
            public void addFieldMappers(Collection<FieldMapper> fieldMappers) {
                // no-op
            }

            @Override
            public void addObjectMappers(Collection<ObjectMapper> objectMappers) {
                // no-op
            }

            @Override
            public Collection<FieldMapper> getNewFieldMappers() {
                throw new UnsupportedOperationException("Strict merge result does not support new field mappers");
            }

            @Override
            public Collection<ObjectMapper> getNewObjectMappers() {
                throw new UnsupportedOperationException("Strict merge result does not support new object mappers");
            }

            @Override
            public void addConflict(String mergeFailure) {
                throw new MapperParsingException("Merging dynamic updates triggered a conflict: " + mergeFailure);
            }
        };
    }

    /**
     * Merge {@code mergeWith} into {@code mergeTo}. Note: this method only
     * merges mappings, not lookup structures. Conflicts are returned as exceptions.
     */
    public static void merge(Mapper mergeInto, Mapper mergeWith) {
        mergeInto.merge(mergeWith, newStrictMergeResult());
    }

    /**
     * Merge {@code mergeWith} into {@code mergeTo}. Note: this method only
     * merges mappings, not lookup structures. Conflicts are returned as exceptions.
     */
    public static void merge(Mapping mergeInto, Mapping mergeWith) {
        mergeInto.merge(mergeWith, newStrictMergeResult());
    }

    /** Split mapper and its descendants into object and field mappers. */
    public static void collect(Mapper mapper, Collection<ObjectMapper> objectMappers, Collection<FieldMapper> fieldMappers) {
        if (mapper instanceof RootObjectMapper) {
            // root mapper isn't really an object mapper
        } else if (mapper instanceof ObjectMapper) {
            objectMappers.add((ObjectMapper)mapper);
        } else if (mapper instanceof FieldMapper) {
            fieldMappers.add((FieldMapper)mapper);
        }
        for (Mapper child : mapper) {
            collect(child, objectMappers, fieldMappers);
        }
    }
}
