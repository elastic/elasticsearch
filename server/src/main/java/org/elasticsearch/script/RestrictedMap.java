/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.script;

import java.util.Map;
import java.util.Set;

/**
 * A class that implements Map-like access without the full Map api.
 *
 * Updates to this class only occur in {@link #put(String, Object)} and {@link #remove(String)}, making
 * it easier to implement than the {@link Map} interface, which allows updates through the
 * {@link Map#entrySet()} and removal through the entry set iterator along with put and remove.
 */
public interface RestrictedMap {
    boolean isAvailable(String key);

    Object put(String key, Object value);

    boolean containsKey(String key);

    boolean containsValue(Object value);

    Object get(String key);

    Object remove(String key);

    Set<String> keySet();

    int size();

    RestrictedMap clone();

    Map<String, Object> asMap();

    void clear();
}
