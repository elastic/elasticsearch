/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.action.fieldcaps;

import org.elasticsearch.index.IndexMode;
import org.elasticsearch.test.ESTestCase;

import java.util.Map;

public class FieldCapsCacheTests extends ESTestCase {

    public void testKey() {
        FieldCapsCache cache = new FieldCapsCache();
        FieldCapsCache.Key key = new FieldCapsCache.Key("uuid", 2, 3, new String[] { "_index" }, new String[] { "-nested" });
        FieldCapabilitiesIndexResponse response = response(2, 3);
        cache.put(key, response);

        assertSame(response, cache.get(key));
        assertNull(cache.get(new FieldCapsCache.Key("other", 2, 3, key.fields(), key.filters())));
        assertNull(cache.get(new FieldCapsCache.Key("uuid", 1, 3, key.fields(), key.filters())));
        assertNull(cache.get(new FieldCapsCache.Key("uuid", 2, 4, key.fields(), key.filters())));
        assertNull(cache.get(new FieldCapsCache.Key("uuid", 2, 3, new String[] { "other" }, key.filters())));
        assertNull(cache.get(new FieldCapsCache.Key("uuid", 2, 3, key.fields(), new String[0])));
    }

    public void testUsesResponseVersions() {
        FieldCapsCache cache = new FieldCapsCache();
        FieldCapsCache.Key key = new FieldCapsCache.Key("uuid", 1, 1, new String[] { "_index" }, new String[0]);
        FieldCapabilitiesIndexResponse response = response(2, 3);
        cache.put(key, response);

        assertNull(cache.get(key));
        assertSame(response, cache.get(new FieldCapsCache.Key("uuid", 2, 3, key.fields(), key.filters())));
    }

    private static FieldCapabilitiesIndexResponse response(long settingsVersion, long mappingVersion) {
        return new FieldCapabilitiesIndexResponse("index", null, Map.of(), true, IndexMode.STANDARD, 1, settingsVersion, mappingVersion);
    }
}
