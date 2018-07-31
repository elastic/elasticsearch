/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.sql.analysis.index;

import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.sql.type.EsField;
import org.elasticsearch.xpack.sql.type.TypesTests;

import java.util.Arrays;
import java.util.Map;

public class IndexResolverTests extends ESTestCase {

    public void testMergeSameMapping() throws Exception {
        Map<String, EsField> oneMapping = TypesTests.loadMapping("mapping-basic.json", true);
        Map<String, EsField> sameMapping = TypesTests.loadMapping("mapping-basic.json", true);
        assertNotSame(oneMapping, sameMapping);
        assertEquals(oneMapping, sameMapping);

        String wildcard = "*";
        IndexResolution resolution = IndexResolver.merge(
                Arrays.asList(IndexResolution.valid(new EsIndex("a", oneMapping)), IndexResolution.valid(new EsIndex("b", sameMapping))),
                wildcard);

        assertTrue(resolution.isValid());

        EsIndex esIndex = resolution.get();

        assertEquals(wildcard, esIndex.name());
        assertEquals(sameMapping, esIndex.mapping());
    }

    public void testMergeDifferentMapping() throws Exception {
        Map<String, EsField> oneMapping = TypesTests.loadMapping("mapping-basic.json", true);
        Map<String, EsField> sameMapping = TypesTests.loadMapping("mapping-basic.json", true);
        Map<String, EsField> differentMapping = TypesTests.loadMapping("mapping-numeric.json", true);

        assertNotSame(oneMapping, sameMapping);
        assertEquals(oneMapping, sameMapping);
        assertNotEquals(oneMapping, differentMapping);

        String wildcard = "*";
        IndexResolution resolution = IndexResolver.merge(
                Arrays.asList(IndexResolution.valid(new EsIndex("a", oneMapping)),
                        IndexResolution.valid(new EsIndex("b", sameMapping)),
                        IndexResolution.valid(new EsIndex("diff", differentMapping))),
                wildcard);

        assertFalse(resolution.isValid());

        MappingException ex = expectThrows(MappingException.class, () -> resolution.get());
        assertEquals(
                "[*] points to indices [a] and [diff] which have different mappings. "
                        + "When using multiple indices, the mappings must be identical.",
                ex.getMessage());
    }
}
