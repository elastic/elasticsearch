/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.ml.inference.rescorer;

import org.apache.lucene.index.LeafReaderContext;

import java.io.IOException;
import java.util.List;
import java.util.Map;

public interface FeatureExtractor {
    void setNextReader(LeafReaderContext segmentContext) throws IOException;

    void addFeatures(Map<String, Object> featureMap, int docId) throws IOException;

    List<String> featureNames();
}
