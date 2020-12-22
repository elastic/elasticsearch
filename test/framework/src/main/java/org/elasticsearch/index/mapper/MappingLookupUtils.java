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

import org.elasticsearch.index.analysis.NamedAnalyzer;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

public class MappingLookupUtils {
    public static MappingLookup fromTypes(MappedFieldType... types) {
        return fromTypes(Arrays.asList(types));
    }

    public static MappingLookup fromTypes(List<MappedFieldType> fields) {
        List<FieldMapper> mappers = new ArrayList<>();
        List<RuntimeFieldType> runtimeFields = new ArrayList<>();
        for (MappedFieldType type : fields) {
            if (type instanceof RuntimeFieldType) {
                runtimeFields.add((RuntimeFieldType) type);
            } else {
                mappers.add(mockFieldMapper(type));
            }
        }
        return new MappingLookup("_doc", mappers, List.of(), List.of(), runtimeFields, 0, souceToParse -> null, true);
    }

    public static FieldMapper mockFieldMapper(MappedFieldType type) {
        Map<String, NamedAnalyzer> indexAnalyzers = Map.of();
        if (type.getTextSearchInfo() != TextSearchInfo.NONE) {
            indexAnalyzers = Map.of(type.name(), type.getTextSearchInfo().getSearchAnalyzer());
        }
        return new MockFieldMapper(type, indexAnalyzers);
    }
}
