/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

/**
 * ColumNAR: a columnar, adaptively-encoded Lucene {@code DocValuesFormat}. See the module
 * {@code README.md} for the architecture and {@code docs/PLAN.md} for the roadmap.
 *
 * <p>Readers of the codec can report read profiling metrics to the codec profiler. The exported API takes a
 * {@code MetricRecorder}, so this module requires the profiler transitively. The package documentation of
 * {@code org.elasticsearch.columnar} describes what is measured and what each metric means.
 */
module org.elasticsearch.columnar {
    // transitive: the exported API returns and accepts Lucene types (DocIdSetIterator, IndexInput,
    // LongValues, ...), so consumers of this module must be able to read lucene.core too.
    requires transitive org.apache.lucene.core;
    requires org.elasticsearch.simdvec;
    requires org.elasticsearch.zstd;
    requires org.elasticsearch.lucene.store;
    requires org.elasticsearch.base;
    requires transitive org.elasticsearch.codec.profiler;

    exports org.elasticsearch.columnar;
    exports org.elasticsearch.columnar.substrate;
    exports org.elasticsearch.columnar.numeric;
    exports org.elasticsearch.columnar.string;

    provides org.apache.lucene.codecs.DocValuesFormat with org.elasticsearch.columnar.ColumNARDocValuesFormat;
}
