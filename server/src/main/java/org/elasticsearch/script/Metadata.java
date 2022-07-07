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
 * Ingest and update metadata available to write scripts
 */
public abstract class Metadata {
    public abstract Object put(String key, Object value);

    public abstract boolean isMetadata(Object key);

    public abstract Object get(Object key);

    public abstract Object remove(Object key);

    public abstract boolean exists(Object key);

    public abstract List<String> keys();

    public abstract int size();
}
