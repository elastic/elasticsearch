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
 */
module org.elasticsearch.columnar {
    // transitive: the exported API returns and accepts Lucene types (DocIdSetIterator, IndexInput,
    // LongValues, ...), so consumers of this module must be able to read lucene.core too.
    requires transitive org.apache.lucene.core;
    requires org.elasticsearch.simdvec;
    requires org.elasticsearch.nativeaccess;
    requires org.elasticsearch.lucene.store;
    requires org.elasticsearch.base;

    exports org.elasticsearch.columnar;
    exports org.elasticsearch.columnar.substrate;
    exports org.elasticsearch.columnar.numeric;

    provides org.apache.lucene.codecs.DocValuesFormat with org.elasticsearch.columnar.ColumNARDocValuesFormat;
}
