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

import org.apache.logging.log4j.core.LogEvent;
import org.apache.logging.log4j.core.config.plugins.Plugin;
import org.apache.logging.log4j.core.config.plugins.PluginAttribute;
import org.apache.logging.log4j.core.config.plugins.PluginFactory;
import org.apache.logging.log4j.core.layout.AbstractStringLayout;
import org.apache.logging.log4j.core.layout.ByteBufferDestination;
import org.apache.logging.log4j.core.layout.PatternLayout;

import java.nio.charset.Charset;
import java.util.Map;

@Plugin(name = "ESJsonLayout", category = "Core", elementType = "layout", printObject = true)
public class ESJsonLayout extends AbstractStringLayout {

    private static final String PATTERN = "{" +
        "\"type\": \"console\", " +
        "\"timestamp\": \"%d{yyyy-MM-dd'T'HH:mm:ss,SSSZ}\", " +
        "\"level\": \"%p\", " +
        "\"class\": \"%c{1.}\", " +
        "\"cluster.name\": \"${sys:es.logs.cluster_name}\", " +
        "\"node.name\": \"%node_name\", " +
        "%node_and_cluster_id " +
        "\"message\": \"%enc{%.-10000m}{JSON}\" " +
        "%exceptionAsJson " +
        "}%n";

    private final PatternLayout patternLayout;

    protected ESJsonLayout(boolean locationInfo, boolean properties, boolean complete,
                           Charset charset) {
        super(charset);
        this.patternLayout = PatternLayout.newBuilder()
            .withPattern(PATTERN)
            .withAlwaysWriteExceptions(false)
            .build();
    }

    @PluginFactory
    public static ESJsonLayout createLayout(@PluginAttribute("locationInfo") boolean locationInfo,
                                            @PluginAttribute("properties") boolean properties,
                                            @PluginAttribute("complete") boolean complete,
                                            @PluginAttribute(value = "charset", defaultString = "UTF-8") Charset charset) {
        return new ESJsonLayout(locationInfo, properties, complete, charset);
    }

    @Override
    public String toSerializable(final LogEvent event) {
        return patternLayout.toSerializable(event);
    }

    @Override
    public Map<String, String> getContentFormat() {
        return patternLayout.getContentFormat();
    }

    @Override
    public void encode(final LogEvent event, final ByteBufferDestination destination) {
        patternLayout.encode(event, destination);
    }

    @Override
    public String toString() {
        return patternLayout.toString();
    }
}
