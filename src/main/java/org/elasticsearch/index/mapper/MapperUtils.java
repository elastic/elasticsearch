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

import org.elasticsearch.common.Strings;
import org.elasticsearch.index.mapper.object.ObjectMapper;

import java.io.IOException;
import java.util.Collection;

public enum MapperUtils {
    ;

    /**
     * Parse the given {@code context} with the given {@code mapper} and apply
     * the potential mapping update in-place. This method is useful when
     * composing mapping updates.
     */
    public static <M extends Mapper> M parseAndMergeUpdate(M mapper, ParseContext context) throws IOException {
        final Mapper update = mapper.parse(context);
        if (update != null) {
            merge(mapper, update);
        }
        return mapper;
    }

    private static MergeResult newStrictMergeContext() {
        return new MergeResult(false) {

            @Override
            public boolean hasConflicts() {
                return false;
            }

            @Override
            public String[] buildConflicts() {
                return Strings.EMPTY_ARRAY;
            }

            @Override
            public void addObjectMappers(Collection<ObjectMapper> objectMappers) {
                // no-op
            }

            @Override
            public void addFieldMappers(Collection<FieldMapper<?>> fieldMappers) {
                // no-op
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
        mergeInto.merge(mergeWith, newStrictMergeContext());
    }

    /**
     * Merge {@code mergeWith} into {@code mergeTo}. Note: this method only
     * merges mappings, not lookup structures. Conflicts are returned as exceptions.
     */
    public static void merge(Mapping mergeInto, Mapping mergeWith) {
        mergeInto.merge(mergeWith, newStrictMergeContext());
    }

}
