/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.index.mapper;

import java.util.Arrays;
import java.util.List;

import static java.util.stream.Collectors.toList;

public class MappingLookupUtils {
    public static MappingLookup fromTypes(MappedFieldType... types) {
        return fromTypes(Arrays.asList(types), org.elasticsearch.common.collect.List.of());
    }

    public static MappingLookup fromTypes(List<MappedFieldType> concreteFields, List<RuntimeFieldType> runtimeFields) {
        List<FieldMapper> mappers = concreteFields.stream().map(MockFieldMapper::new).collect(toList());
        return new MappingLookup(
            "_doc",
            mappers,
            org.elasticsearch.common.collect.List.of(),
            org.elasticsearch.common.collect.List.of(),
            runtimeFields,
            0,
            souceToParse -> null,
            true
        );
    }
}
