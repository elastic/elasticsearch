/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.ml.inference;

import java.util.Map;

public interface ModelLoader {
    Model load(String modelId, String processorTag, boolean ignoreMissing,
               Map<String, Object> config) throws Exception;

    // parses the config out of the map
    void consumeConfiguration(String processorTag, Map<String, Object> config);
}
