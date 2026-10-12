/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.mapper;

import org.apache.lucene.util.BytesRef;
import org.elasticsearch.common.settings.Settings;

import java.io.IOException;
import java.util.List;
import java.util.Map;

public class FallbackSyntheticSourceBlockLoaderTests extends MapperServiceTestCase {

    public void testObjectAndDottedNameInParentKeptInIgnoredSource() throws IOException {
        var params = new BlockLoaderTestCase.Params(true, MappedFieldType.FieldExtractPreference.NONE);
        var mapping = mapping(b -> {
            b.startObject("obj").field("type", "object").field("synthetic_source_keep", "all");
            {
                b.startObject("properties");
                b.startObject("sub").field("type", "object");
                {
                    b.startObject("properties");
                    b.startObject("field").field("type", "keyword").field("doc_values", false).endObject();
                    b.endObject();
                }
                b.endObject();
                b.endObject();
            }
            b.endObject();
        });
        var settings = Settings.builder().put("index.mapping.source.mode", "synthetic");
        MapperService mapperService = createMapperService(settings.build(), mapping);

        Map<String, Object> document = Map.of("obj", Map.of("sub", Map.of("field", "a", "fielx", "x"), "other", "y", "sub.field", "b"));
        new BlockLoaderTestRunner(params).runTest(mapperService, document, List.of(new BytesRef("a"), new BytesRef("b")), "obj.sub.field");
    }
}
