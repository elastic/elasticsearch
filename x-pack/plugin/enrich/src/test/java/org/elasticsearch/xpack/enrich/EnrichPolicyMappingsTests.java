/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.enrich;

import org.elasticsearch.core.Strings;
import org.elasticsearch.index.mapper.MapperRegistry;
import org.elasticsearch.indices.IndicesModule;
import org.elasticsearch.test.ESTestCase;

import java.util.List;

public class EnrichPolicyMappingsTests extends ESTestCase {

    public void testAggregationsVsTransforms() {
        // Note: if a new plugin is added, it must be added here
        IndicesModule indicesModule = new IndicesModule(List.of());
        MapperRegistry mapperRegistry = indicesModule.getMapperRegistry();

        List<String> fieldTypes = mapperRegistry.getMapperParsers().keySet().stream().toList();

        for (String fieldType : fieldTypes) {
            String message = Strings.format("""
                The following field type is unknown to enrich: [%s]. If this is a newly added field type, \
                please open an issue to add enrich support for it. Afterwards add "%s" to the list in %s. \
                Thanks!\
                """, fieldType, fieldType, EnrichPolicyMappings.class.getName());
            assertTrue(
                message,
                EnrichPolicyMappings.isSupportedByEnrich(fieldType)
                    || EnrichPolicyMappings.isUnsupportedByEnrich(fieldType)
            );
        }
    }
}
