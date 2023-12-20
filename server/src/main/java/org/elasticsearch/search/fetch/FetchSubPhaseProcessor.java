/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.search.fetch;

import org.apache.lucene.index.LeafReaderContext;
import org.elasticsearch.search.fetch.FetchSubPhase.HitContext;

import java.io.IOException;
import java.util.Map;

/**
 * Executes the logic for a {@link FetchSubPhase} against a particular leaf reader and hit
 */
public interface FetchSubPhaseProcessor {

    /**
     * Called when moving to the next {@link LeafReaderContext} for a set of hits
     */
    void setNextReader(LeafReaderContext readerContext) throws IOException;

    /**
     * Called in doc id order for each hit in a leaf reader
     */
    void process(HitContext hitContext) throws IOException;

    /**
     * Called when profiling after processing all documents to get any extra
     * debug information the phase collected.
     */
    default Map<String, Object> getDebugInfo() {
        return null;
    }

    /**
     * The stored fields or source required by this sub phase
     */
    StoredFieldsSpec storedFieldsSpec();
}
