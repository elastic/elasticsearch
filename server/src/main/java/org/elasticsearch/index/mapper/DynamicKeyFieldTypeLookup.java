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

import org.elasticsearch.common.collect.CopyOnWriteHashMap;

import java.util.Collections;
import java.util.Iterator;
import java.util.Map;

/**
 * A container that supports looking up field types for 'dynamic key' fields ({@link DynamicKeyFieldMapper}).
 *
 * Compared to standard fields, 'dynamic key' fields require special handling. Given a field name of the form
 * 'path_to_field.path_to_key', the container will dynamically return a new {@link MappedFieldType} that is
 * suitable for performing searches on the sub-key.
 *
 * Note: we anticipate that 'flattened' fields will be the only implementation {@link DynamicKeyFieldMapper}.
 * Flattened object fields live in the 'mapper-flattened' module.
 */
class DynamicKeyFieldTypeLookup {
    private final CopyOnWriteHashMap<String, DynamicKeyFieldMapper> mappers;
    private final Map<String, String> aliasToConcreteName;

    /**
     * The maximum field depth of any dynamic key mapper. Allows us to stop searching for
     * a dynamic key mapper as soon as we've passed the maximum possible field depth.
     */
    private final int maxKeyDepth;

    DynamicKeyFieldTypeLookup() {
        this.mappers = new CopyOnWriteHashMap<>();
        this.aliasToConcreteName = Collections.emptyMap();
        this.maxKeyDepth = 0;
    }

    private DynamicKeyFieldTypeLookup(CopyOnWriteHashMap<String, DynamicKeyFieldMapper> mappers,
                                      Map<String, String> aliasToConcreteName,
                                      int maxKeyDepth) {
        this.mappers = mappers;
        this.aliasToConcreteName = aliasToConcreteName;
        this.maxKeyDepth = maxKeyDepth;
    }

    DynamicKeyFieldTypeLookup copyAndAddAll(Map<String, DynamicKeyFieldMapper> newMappers,
                                            Map<String, String> aliasToConcreteName) {
        CopyOnWriteHashMap<String, DynamicKeyFieldMapper> combinedMappers = this.mappers.copyAndPutAll(newMappers);
        int maxKeyDepth = getMaxKeyDepth(combinedMappers, aliasToConcreteName);
        return new DynamicKeyFieldTypeLookup(combinedMappers, aliasToConcreteName, maxKeyDepth);
    }

    /**
     * Check if the given field corresponds to a dynamic key mapper of the
     * form 'path_to_field.path_to_key'. If so, returns a field type that
     * can be used to perform searches on this field. Otherwise returns null.
     */
    MappedFieldType get(String field) {
        if (mappers.isEmpty()) {
            return null;
        }

        int dotIndex = -1;
        int fieldDepth = 0;

        while (true) {
            if (++fieldDepth > maxKeyDepth) {
                return null;
            }

            dotIndex = field.indexOf('.', dotIndex + 1);
            if (dotIndex < 0) {
                return null;
            }

            String parentField = field.substring(0, dotIndex);
            String concreteField = aliasToConcreteName.getOrDefault(parentField, parentField);
            DynamicKeyFieldMapper mapper = mappers.get(concreteField);

            if (mapper != null) {
                String key = field.substring(dotIndex + 1);
                return mapper.keyedFieldType(key);
            }
        }
    }

    Iterator<MappedFieldType> fieldTypes() {
        return mappers.values().stream()
            .<MappedFieldType>map(mapper -> mapper.keyedFieldType(""))
            .iterator();
    }

    // Visible for testing.
    static int getMaxKeyDepth(Map<String, DynamicKeyFieldMapper> dynamicKeyMappers,
                              Map<String, String> aliasToConcreteName) {
        int maxFieldDepth = 0;
        for (Map.Entry<String, String> entry : aliasToConcreteName.entrySet()) {
            String aliasName = entry.getKey();
            String path = entry.getValue();
            if (dynamicKeyMappers.containsKey(path)) {
                maxFieldDepth = Math.max(maxFieldDepth, fieldDepth(aliasName));
            }
        }

        for (String fieldName : dynamicKeyMappers.keySet()) {
            if (dynamicKeyMappers.containsKey(fieldName)) {
                maxFieldDepth = Math.max(maxFieldDepth, fieldDepth(fieldName));
            }
        }

        return maxFieldDepth;
    }

    /**
     * Computes the total depth of this field by counting the number of parent fields
     * in its path. As an example, the field 'parent1.parent2.field' has depth 3.
     */
    private static int fieldDepth(String field) {
        int numDots = 0;
        int dotIndex = -1;
        while (true) {
            dotIndex = field.indexOf('.', dotIndex + 1);
            if (dotIndex < 0) {
                break;
            }
            numDots++;
        }
        return numDots + 1;
    }
}
