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

import org.elasticsearch.common.component.AbstractComponent;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.node.Node;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CopyOnWriteArrayList;

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

    public Map<String, String> buildAttributes() {
        Map<String, String> attributes = new HashMap<>(Node.NODE_ATTRIBUTES.get(this.settings).getAsMap());
        attributes.remove("name"); // name is extracted in other places
        if (attributes.containsKey("client")) {
            throw new IllegalArgumentException("node.client setting is no longer supported, use " + Node.NODE_MASTER_SETTING.getKey()
                + ", " + Node.NODE_DATA_SETTING.getKey() + " and " + Node.NODE_INGEST_SETTING.getKey() + " explicitly instead");
        }
        //nocommit why don't we remove master as well if it's true? and ingest?
        if (attributes.containsKey(DiscoveryNode.Role.DATA.getRoleName())) {
            if (attributes.get(DiscoveryNode.Role.DATA.getRoleName()).equals("true")) {
                attributes.remove(DiscoveryNode.Role.DATA.getRoleName());
            }
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

        return attributes;
    }

    public interface CustomAttributesProvider {

        Map<String, String> buildAttributes();
    }
}
