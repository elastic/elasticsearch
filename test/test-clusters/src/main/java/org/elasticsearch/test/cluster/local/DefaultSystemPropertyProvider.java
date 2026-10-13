/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.test.cluster.local;

import org.elasticsearch.test.cluster.SystemPropertyProvider;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

public class DefaultSystemPropertyProvider implements SystemPropertyProvider {
    @Override
    public Map<String, String> get(LocalClusterSpec.LocalNodeSpec nodeSpec) {
        Map<String, String> properties = new HashMap<>();
        properties.put("ingest.geoip.downloader.enabled.default", "false");

        // enable test features unless we are running forwards compatibility tests
        if (Boolean.parseBoolean(System.getProperty("tests.fwc", "false")) == false) {
            properties.put("tests.testfeatures.enabled", "true");
        }

        // DLM frozen tier serialization is gated on both a feature flag and a pre-GA transport
        // version (searchable_snapshots_dlm, within the 9.4 line's range), so snapshot builds
        // (flag on by default) and release builds (flag off) disagree on the wire format when
        // communicating with a 9.4.x node. Force the flag off on every test cluster node so
        // serialization is consistent regardless of build type.
        // This guard is skipped when the cluster spec requests the flag via feature(FeatureFlag.X)
        // to avoid overriding an explicit enable: feature-flag properties are emitted earlier
        // in ES_JAVA_OPTS than resolved system properties, and the JVM takes the last -D value.
        // A per-suite .systemProperty(...) call also overrides this default via map precedence.
        // See https://github.com/elastic/elasticsearch/issues/156594.
        String dlmFlagProperty = "es.dlm_searchable_snapshots_feature_flag_enabled";
        boolean dlmFlagRequested = nodeSpec.getFeatures().stream().anyMatch(f -> f.systemProperty.startsWith(dlmFlagProperty + "="));
        if (dlmFlagRequested == false) {
            properties.put(dlmFlagProperty, "false");
        }

        return Collections.unmodifiableMap(properties);
    }
}
