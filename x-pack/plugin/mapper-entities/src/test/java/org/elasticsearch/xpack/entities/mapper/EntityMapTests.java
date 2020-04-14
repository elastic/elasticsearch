/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.entities.mapper;

import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.BytesRefIterator;
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.test.ESTestCase;

import java.io.IOException;

public class EntityMapTests extends ESTestCase {

    public void testSimpleEntry() throws IOException {
        EntityMap map = new EntityMap.Builder(BigArrays.NON_RECYCLING_INSTANCE)
            .addEntity(new BytesRef("foo"), new BytesRef("bar"), new BytesRef("baz"))
            .addEntity(new BytesRef("hoo"), new BytesRef("har"), new BytesRef("haz"))
            .build();

        assertEquals(new BytesRef("foo"), map.findCanonicalEntity(new BytesRef("bar")));
        assertEquals(new BytesRef("foo"), map.findCanonicalEntity(new BytesRef("baz")));

        BytesRefIterator it = map.findSynonyms(new BytesRef("foo"));
        assertNotNull(it);
        assertEquals(new BytesRef("foo"), it.next());
        assertEquals(new BytesRef("bar"), it.next());
        assertEquals(new BytesRef("baz"), it.next());
        assertNull(it.next());
    }

}
