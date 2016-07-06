/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.elasticsearch.cluster.node;

import org.elasticsearch.Version;
import org.elasticsearch.common.component.AbstractComponent;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.TransportAddress;
import org.elasticsearch.node.Node;

import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.function.Supplier;

/**
 */
public class DiscoveryNodeService extends AbstractComponent {

    private final List<CustomAttributesProvider> customAttributesProviders = new CopyOnWriteArrayList<>();

    @Inject
    public DiscoveryNodeService(Settings settings) {
        super(settings);
    }

    public DiscoveryNodeService addCustomAttributeProvider(CustomAttributesProvider customAttributesProvider) {
        customAttributesProviders.add(customAttributesProvider);
        return this;
    }

    public DiscoveryNode buildLocalNode(TransportAddress publishAddress, Supplier<String> nodeIdSupplier) {
        Map<String, String> attributes = new HashMap<>(Node.NODE_ATTRIBUTES.get(this.settings).getAsMap());
        Set<DiscoveryNode.Role> roles = new HashSet<>();
        if (Node.NODE_INGEST_SETTING.get(settings)) {
            roles.add(DiscoveryNode.Role.INGEST);
        }
        if (Node.NODE_MASTER_SETTING.get(settings)) {
            roles.add(DiscoveryNode.Role.MASTER);
        }
        if (Node.NODE_DATA_SETTING.get(settings)) {
            roles.add(DiscoveryNode.Role.DATA);
        }

        for (CustomAttributesProvider provider : customAttributesProviders) {
            try {
                Map<String, String> customAttributes = provider.buildAttributes();
                if (customAttributes != null) {
                    for (Map.Entry<String, String> entry : customAttributes.entrySet()) {
                        if (!attributes.containsKey(entry.getKey())) {
                            attributes.put(entry.getKey(), entry.getValue());
                        }
                    }
                }
            } catch (Exception e) {
                logger.warn("failed to build custom attributes from provider [{}]", e, provider);
            }
        }
        return new DiscoveryNode(Node.NODE_NAME_SETTING.get(settings), nodeIdSupplier.get(), publishAddress, attributes, roles,
            Version.CURRENT);
    }

    public interface CustomAttributesProvider {

        Map<String, String> buildAttributes();
    }
}
