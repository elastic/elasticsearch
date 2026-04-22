/*
 * ELASTICSEARCH CONFIDENTIAL
 * __________________
 *
 * Copyright Elasticsearch B.V. All rights reserved.
 *
 * NOTICE:  All information contained herein is, and remains
 * the property of Elasticsearch B.V. and its suppliers, if any.
 * The intellectual and technical concepts contained herein
 * are proprietary to Elasticsearch B.V. and its suppliers and
 * may be covered by U.S. and Foreign Patents, patents in
 * process, and are protected by trade secret or copyright
 * law.  Dissemination of this information or reproduction of
 * this material is strictly forbidden unless prior written
 * permission is obtained from Elasticsearch B.V.
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
