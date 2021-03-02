/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
package org.elasticsearch.test.hamcrest;

import org.elasticsearch.common.collect.ImmutableOpenMap;
import org.hamcrest.Matcher;

/**
 * Assertions for easier handling of our custom collections,
 * for example ImmutableOpenMap
 */
public class CollectionAssertions {

    public static Matcher<ImmutableOpenMap<String, ?>> hasKey(final String key) {
        return new CollectionMatchers.ImmutableOpenMapHasKeyMatcher(key);
    }

    public static Matcher<ImmutableOpenMap<String, ?>> hasAllKeys(final String... keys) {
        return new CollectionMatchers.ImmutableOpenMapHasAllKeysMatcher(keys);
    }
}
