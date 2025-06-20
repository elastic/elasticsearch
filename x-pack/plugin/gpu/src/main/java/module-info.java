/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

/** Provides GPU-accelerated support for vector search. */
module org.elasticsearch.gpu {
    requires org.apache.lucene.core;
    requires org.elasticsearch.xcontent;
    requires org.elasticsearch.server;
    requires org.elasticsearch.base;
    requires com.nvidia.cuvs;

    provides org.apache.lucene.codecs.KnnVectorsFormat with org.elasticsearch.xpack.gpu.codec.GPUVectorsFormat;
}
