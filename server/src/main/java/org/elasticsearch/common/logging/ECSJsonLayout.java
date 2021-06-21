/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
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
public class ECSJsonLayout {

    @PluginBuilderFactory
    public static ECSJsonLayout.Builder newBuilder() {
        return new ECSJsonLayout.Builder().asBuilder();
    }

    public static class Builder extends AbstractStringLayout.Builder<Builder>
        implements org.apache.logging.log4j.core.util.Builder<EcsLayout> {

        @PluginAttribute("dataset")
        String dataset;

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
            return new KeyValuePair[] {
                new KeyValuePair("event.dataset", dataset),
                new KeyValuePair("trace.id", "%trace_id"),
                new KeyValuePair("elasticsearch.cluster.uuid", "%cluster_id"),
                new KeyValuePair("elasticsearch.node.id", "%node_id"),
                new KeyValuePair("elasticsearch.node.name", "%ESnode_name"),
                new KeyValuePair("elasticsearch.cluster.name", "${sys:es.logs.cluster_name}"), };
    }

        public String getDataset() {
            return dataset;
        }

        public Builder setDataset(final String dataset) {
            this.dataset = dataset;
            return asBuilder();
        }
    }
}
