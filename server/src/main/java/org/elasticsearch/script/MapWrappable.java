/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.script;

import java.util.List;

/**
 * A class that can easily be wrapped in a Map interface by {@link MapWrapper}
 */
public interface MapWrappable {
    boolean ownsKey(String key);
    Object put(String key, Object value);
    boolean containsKey(String key);
    boolean containsValue(Object value);
    Object get(String key);
    Object remove(String key);
    List<String> keys();
    int size();
    MapWrappable clone();
}
