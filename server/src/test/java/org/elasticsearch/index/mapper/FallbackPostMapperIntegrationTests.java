/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.mapper;

import java.io.IOException;

public class FallbackPostMapperIntegrationTests extends MapperServiceTestCase {

    public void testCopyToDestinationWithIgnoreMalformedPreservesValueInSyntheticSource() throws IOException {
        DocumentMapper mapper = createSytheticSourceMapperService(mapping(b -> {
            b.startObject("src");
            {
                b.field("type", "keyword");
                b.field("copy_to", "dest");
            }
            b.endObject();
            b.startObject("dest");
            {
                b.field("type", "integer");
                b.field("ignore_malformed", true);
            }
            b.endObject();
        })).documentMapper();

        String syntheticSource = syntheticSource(mapper, b -> {
            b.field("src", "123");
            b.field("dest", "not-a-number");
        });

        assertEquals("{\"dest\":\"not-a-number\",\"src\":\"123\"}", syntheticSource);
    }

    /**
     * Verifies that {@code source_keep: all + ignore_malformed} still preserves the malformed value in synthetic source.
     */
    public void testSourceKeepAllWithIgnoreMalformedPreservesValueInSyntheticSource() throws IOException {
        DocumentMapper mapper = createSytheticSourceMapperService(
            fieldMapping(b -> b.field("type", "integer").field("synthetic_source_keep", "all").field("ignore_malformed", true))
        ).documentMapper();

        String syntheticSource = syntheticSource(mapper, b -> b.field("field", "not-a-number"));

        assertEquals("{\"field\":\"not-a-number\"}", syntheticSource);
    }
}
