/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.spatial.vectortile.collector;

import org.apache.lucene.search.Collector;


public interface VectorTileCollector extends Collector {

    /**
     * Returns a representation of the vector tile for this shard's results to be returned
     */
    byte[] getVectorTile();
}
