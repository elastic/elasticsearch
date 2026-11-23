/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.codec;

import org.apache.lucene.codecs.Codec;
import org.elasticsearch.test.ESTestCase;

import java.util.HashMap;
import java.util.Map;
import java.util.ServiceLoader;

import static org.hamcrest.Matchers.equalTo;

/**
 * Codec names are written into segment metadata and resolved through SPI when reading, so every
 * registered codec must have a distinct name that resolves back to its own class.
 */
public class ElasticsearchCodecNamesTests extends ESTestCase {

    public void testRegisteredCodecNamesAreUnique() {
        Map<String, Class<?>> classesByName = new HashMap<>();
        for (Codec codec : ServiceLoader.load(Codec.class)) {
            Class<?> previous = classesByName.put(codec.getName(), codec.getClass());
            assertNull("codec name [" + codec.getName() + "] is used by both " + previous + " and " + codec.getClass(), previous);
        }
    }

    public void testRegisteredCodecNamesResolveToOwnClass() {
        for (Codec codec : ServiceLoader.load(Codec.class)) {
            assertThat("codec name [" + codec.getName() + "]", Codec.forName(codec.getName()).getClass(), equalTo(codec.getClass()));
        }
    }
}
