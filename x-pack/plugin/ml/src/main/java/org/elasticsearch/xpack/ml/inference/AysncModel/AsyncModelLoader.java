/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.ml.inference.AysncModel;

import org.elasticsearch.xpack.ml.inference.Model;
import org.elasticsearch.xpack.ml.inference.ModelLoader;

import java.util.Map;

public class AsyncModelLoader implements ModelLoader {

    @Override
    public Model load(String modelId, String processorTag, boolean ignoreMissing, Map<String, Object> config) throws Exception {
        return null;
    }

    @Override
    public void consumeConfiguration(String processorTag, Map<String, Object> config) {

    }
}
