/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.rollup;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.xpack.core.XPackFeatureSet;
import org.elasticsearch.xpack.core.XPackField;
import org.elasticsearch.xpack.core.rollup.RollupFeatureSetUsage;

import java.util.Map;

public class RollupFeatureSet implements XPackFeatureSet {

    @Override
    public String name() {
        return XPackField.ROLLUP;
    }

    @Override
    public boolean available() {
        return true;
    }

    @Override
    public boolean enabled() {
        return true;
    }

    @Override
    public Map<String, Object> nativeCodeInfo() {
        return null;
    }

    @Override
    public void usage(ActionListener<XPackFeatureSet.Usage> listener) {
        // TODO expose the currently running rollup tasks on this node? Unclear the best way to do that
        listener.onResponse(new RollupFeatureSetUsage());
    }
}
