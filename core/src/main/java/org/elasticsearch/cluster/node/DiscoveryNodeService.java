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
import org.elasticsearch.common.Randomness;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.component.AbstractComponent;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Setting.Property;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.TransportAddress;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.CopyOnWriteArrayList;

/**
 */
public class DiscoveryNodeService extends AbstractComponent {

    public static final Setting<Long> NODE_ID_SEED_SETTING =
            // don't use node.id.seed so it won't be seen as an attribute
            Setting.longSetting("node_id.seed", 0L, Long.MIN_VALUE, Property.NodeScope);
    private final List<CustomAttributesProvider> customAttributesProviders = new CopyOnWriteArrayList<>();
    private final Version version;

    @Inject
    public DiscoveryNodeService(Settings settings, Version version) {
        super(settings);
        this.version = version;
    }

    public static String generateNodeId(Settings settings) {
        Random random = Randomness.get(settings, NODE_ID_SEED_SETTING);
        return Strings.randomBase64UUID(random);
    }

    public DiscoveryNodeService addCustomAttributeProvider(CustomAttributesProvider customAttributesProvider) {
        customAttributesProviders.add(customAttributesProvider);
        return this;
    }

    public DiscoveryNode buildLocalNode(TransportAddress publishAddress) {
        Map<String, String> attributes = new HashMap<>(settings.getByPrefix("node.").getAsMap());
        attributes.remove("name"); // name is extracted in other places
        if (attributes.containsKey("client")) {
            if (attributes.get("client").equals("false")) {
                attributes.remove("client"); // this is the default
            } else {
                // if we are client node, don't store data ...
                attributes.put("data", "false");
            }
        }
        if (attributes.containsKey("data")) {
            if (attributes.get("data").equals("true")) {
                attributes.remove("data");
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

        final String nodeId = generateNodeId(settings);
        return new DiscoveryNode(settings.get("node.name"), nodeId, publishAddress, attributes, version);
    }

    public interface CustomAttributesProvider {

        Map<String, String> buildAttributes();
    }
}
