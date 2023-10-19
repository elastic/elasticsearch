/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.search.internal;

import org.apache.lucene.search.Collector;

import java.io.IOException;

/** A {@link Collector} extension that allows to run a post-collection phase. This phase
 * is run on the same thread as the collection phase. */
public interface TwoPhaseCollector extends Collector {

    /**
     * run post-collection phase
     */
    void doPostCollection() throws IOException;
}
