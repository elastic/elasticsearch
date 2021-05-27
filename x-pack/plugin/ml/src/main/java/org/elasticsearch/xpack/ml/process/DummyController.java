/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.ml.process;

import java.util.Collections;
import java.util.Map;

/**
 * A dummy ML controller for use in internal cluster tests where it
 * is not possible/appropriate to start a native controller.
 */
public class DummyController implements MlController {

    @Override
    public Map<String, Object> getNativeCodeInfo() {
        return Collections.emptyMap();
    }

    @Override
    public void stop() {
        // no-op
    }
}
