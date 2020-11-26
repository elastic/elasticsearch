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

package org.elasticsearch.common.logging;

import co.elastic.logging.log4j2.EcsLayout;
import org.apache.logging.log4j.core.Layout;
import org.apache.logging.log4j.core.config.Node;
import org.apache.logging.log4j.core.config.plugins.Plugin;
import org.apache.logging.log4j.core.config.plugins.PluginAttribute;
import org.apache.logging.log4j.core.config.plugins.PluginBuilderFactory;
import org.apache.logging.log4j.core.layout.AbstractStringLayout;
import org.apache.logging.log4j.core.util.KeyValuePair;

import java.nio.charset.StandardCharsets;

/**
 * This is a wrapper class around <code>co.elastic.logging.log4j2.EcsLayout</code>
 * in order to avoid a duplication of configuration in log4j2.properties
 */
@Plugin(name = "ECSJsonLayout", category = Node.CATEGORY, elementType = Layout.ELEMENT_TYPE, printObject = true)
public class ECSJsonLayout  {

    @PluginBuilderFactory
    public static ECSJsonLayout.Builder newBuilder() {
        return new ECSJsonLayout.Builder().asBuilder();
    }

    public static class Builder extends AbstractStringLayout.Builder<Builder>
        implements org.apache.logging.log4j.core.util.Builder<EcsLayout> {

        @PluginAttribute("type_name")
        String type;

        public Builder() {
            setCharset(StandardCharsets.UTF_8);
        }

        @Override
        public EcsLayout build() {
            return EcsLayout.newBuilder()
                            .setConfiguration(getConfiguration())
                            .setServiceName("ES_ECS")
                            .setStackTraceAsArray(false)
                            .setIncludeMarkers(true)
                            .setAdditionalFields(additionalFields())
                            .build();
        }

        private KeyValuePair[] additionalFields() {
            return new KeyValuePair[]{
                new KeyValuePair("type",type),
                new KeyValuePair("cluster.uuid","%cluster_id"),
                new KeyValuePair("node.id","%node_id"),
                new KeyValuePair("node.name","%ESnode_name"),
                new KeyValuePair("cluster.name","${sys:es.logs.cluster_name}"),
            };
        }

        public String getType() {
            return type;
        }

        public Builder setType(final String type) {
            this.type = type;
            return asBuilder();
        }
    }
}
