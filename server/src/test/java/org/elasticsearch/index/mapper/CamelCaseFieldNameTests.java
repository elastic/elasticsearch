/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.index.mapper;

public class CamelCaseFieldNameTests extends MapperServiceTestCase {

    public void testCamelCaseFieldNameStaysAsIs() throws Exception {

        MapperService mapperService = createMapperService(mapping(b -> {}));

        ParsedDocument doc = mapperService.documentMapper().parse(source(b -> b.field("thisIsCamelCase", "value1")));

        assertNotNull(doc.dynamicMappingsUpdate());

        merge(mapperService, dynamicMapping(doc.dynamicMappingsUpdate()));

        DocumentMapper documentMapper = mapperService.documentMapper();
        assertNotNull(documentMapper.mappers().getMapper("thisIsCamelCase"));
        assertNull(documentMapper.mappers().getMapper("this_is_camel_case"));

        documentMapper = mapperService.merge("_doc", documentMapper.mappingSource(), MapperService.MergeReason.MAPPING_UPDATE);

        assertNotNull(documentMapper.mappers().getMapper("thisIsCamelCase"));
        assertNull(documentMapper.mappers().getMapper("this_is_camel_case"));
    }
}
