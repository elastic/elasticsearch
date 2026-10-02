/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.codec.vectors;

import org.apache.lucene.tests.index.BaseKnnVectorsFormatTestCase;
import org.elasticsearch.common.logging.LogConfigurator;

/**
 * Common superclass for every Elasticsearch {@code KnnVectorsFormat} test, sitting between our
 * {@code getCodec()}-implementing test classes and Lucene's {@link BaseKnnVectorsFormatTestCase}.
 *
 * <p>Anything Elasticsearch-specific that every vector format test should get "for free" belongs here,
 * rather than being duplicated (or, worse, forgotten) in individual {@code *FormatTests} classes: it
 * runs for every format under test simply because the class extends this one, with no per-format
 * opt-in required.
 */
public abstract class ESBaseKnnVectorsFormatTestCase extends BaseKnnVectorsFormatTestCase {

    static {
        LogConfigurator.configureESLogging(); // native access requires logging to be initialized
    }
}
