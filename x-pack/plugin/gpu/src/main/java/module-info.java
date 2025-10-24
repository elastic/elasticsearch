/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

import org.elasticsearch.xpack.gpu.codec.ES92GpuHnswSQVectorsFormat;
import org.elasticsearch.xpack.gpu.codec.ES92GpuHnswVectorsFormat;

/** Provides GPU-accelerated support for vector indexing. */
module org.elasticsearch.gpu {
    requires org.elasticsearch.logging;
    requires org.apache.lucene.core;
    requires org.elasticsearch.xcontent;
    requires org.elasticsearch.server;
    requires org.elasticsearch.base;
    requires com.nvidia.cuvs;

    exports org.elasticsearch.xpack.gpu.codec;

    provides org.elasticsearch.features.FeatureSpecification with org.elasticsearch.xpack.gpu.GPUFeatures;
    provides org.apache.lucene.codecs.KnnVectorsFormat with ES92GpuHnswVectorsFormat, ES92GpuHnswSQVectorsFormat;
}
