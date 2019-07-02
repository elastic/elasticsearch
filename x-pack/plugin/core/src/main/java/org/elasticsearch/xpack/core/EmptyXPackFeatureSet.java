/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.core;

import org.elasticsearch.action.ActionListener;

import java.util.Collections;
import java.util.Map;

public class EmptyXPackFeatureSet implements XPackFeatureSet {
    @Override
    public String name() {
        return "Empty XPackFeatureSet";
    }

    @Override
    public boolean available() {
        return false;
    }

    @Override
    public boolean enabled() {
        return false;
    }

    @Override
    public Map<String, Object> nativeCodeInfo() {
        return Collections.emptyMap();
    }

    @Override
    public void usage(ActionListener<Usage> listener) {

    }
}
