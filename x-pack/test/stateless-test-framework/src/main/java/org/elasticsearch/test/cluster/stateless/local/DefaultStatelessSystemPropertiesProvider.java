/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.test.cluster.stateless.local;

import org.elasticsearch.test.cluster.local.DefaultSystemPropertyProvider;
import org.elasticsearch.test.cluster.local.LocalClusterSpec;

import java.util.HashMap;
import java.util.Map;

public class DefaultStatelessSystemPropertiesProvider extends DefaultSystemPropertyProvider {

    @Override
    public Map<String, String> get(LocalClusterSpec.LocalNodeSpec nodeSpec) {
        Map<String, String> properties = new HashMap<>(super.get(nodeSpec));
        properties.put("tests.testfeatures.enabled", "true");
        // reduce the size of direct buffers so we don't OOM when reserving direct memory on small machines with many CPUs in tests
        properties.put("es.searchable.snapshot.shared_cache.write_buffer.size", "256k");
        return properties;
    }
}
