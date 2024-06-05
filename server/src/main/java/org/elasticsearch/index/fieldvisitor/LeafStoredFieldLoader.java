/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.index.fieldvisitor;

import org.elasticsearch.common.bytes.BytesReference;

import java.io.IOException;
import java.util.List;
import java.util.Map;

/**
 * Loads stored fields for a LeafReader
 *
 * Which stored fields to load will be configured by the loader's parent
 * {@link StoredFieldLoader}
 */
public interface LeafStoredFieldLoader {

    /**
     * Advance the reader to a document.  This should be idempotent.
     */
    void advanceTo(int doc) throws IOException;

    /**
     * @return the source for the current document
     */
    BytesReference source();

    /**
     * @return the ID for the current document
     */
    String id();

    /**
     * @return the routing path for the current document
     */
    String routing();

    /**
     * @return stored fields for the current document
     */
    Map<String, List<Object>> storedFields();

}
