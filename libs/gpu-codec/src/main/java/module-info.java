/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

/** Provides GPU-accelerated support for vector indexing. */
module org.elasticsearch.gpu {
    requires org.elasticsearch.logging;
    requires org.apache.lucene.core;
    requires org.elasticsearch.xcontent;
    requires org.elasticsearch.base;
    requires com.nvidia.cuvs;
    requires org.elasticsearch.server;

    exports org.elasticsearch.gpu;
    exports org.elasticsearch.gpu.codec;

    provides org.apache.lucene.codecs.KnnVectorsFormat
        with
            org.elasticsearch.gpu.codec.ES92GpuHnswVectorsFormat,
            org.elasticsearch.gpu.codec.ES92GpuHnswSQVectorsFormat;
}
