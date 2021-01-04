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

import java.util.Arrays;
import java.util.List;

import static java.util.stream.Collectors.toList;

public class MappingLookupUtils {
    public static MappingLookup fromTypes(MappedFieldType... types) {
        return fromTypes(Arrays.asList(types), List.of());
    }

    public static MappingLookup fromTypes(List<MappedFieldType> concreteFields, List<RuntimeFieldType> runtimeFields) {
        List<FieldMapper> mappers = concreteFields.stream().map(MockFieldMapper::new).collect(toList());
        return new MappingLookup("_doc", mappers, List.of(), List.of(), runtimeFields, 0, souceToParse -> null, true);
    }
}
