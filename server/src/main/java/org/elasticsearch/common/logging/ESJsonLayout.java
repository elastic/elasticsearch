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

import org.apache.logging.log4j.core.Layout;
import org.apache.logging.log4j.core.LogEvent;
import org.apache.logging.log4j.core.config.Node;
import org.apache.logging.log4j.core.config.plugins.Plugin;
import org.apache.logging.log4j.core.config.plugins.PluginAttribute;
import org.apache.logging.log4j.core.config.plugins.PluginFactory;
import org.apache.logging.log4j.core.layout.AbstractStringLayout;
import org.apache.logging.log4j.core.layout.ByteBufferDestination;
import org.apache.logging.log4j.core.layout.PatternLayout;
import org.elasticsearch.common.Strings;

import java.nio.charset.Charset;
import java.util.Map;

/**
 * Formats log events as strings in a json format.
 * <p>
 * The class is wrapping the {@link PatternLayout} with a pattern to format into json. This gives more flexibility and control over how the
 * log messages are formatted in {@link org.apache.logging.log4j.core.layout.JsonLayout}
 */
@Plugin(name = "ESJsonLayout", category = Node.CATEGORY, elementType = Layout.ELEMENT_TYPE, printObject = true)
public class ESJsonLayout extends AbstractStringLayout {
    /**
     * Fields used in a pattern to format a json log line:
     * <ul>
     * <li>type - the type of logs. These represent appenders and help docker distinguish log streams.</li>
     * <li>timestamp - ISO8601 with additional timezone ID</li>
     * <li>level - INFO, WARN etc</li>
     * <li>component - logger name, most of the times class name</li>
     * <li>cluster.name - taken from sys:es.logs.cluster_name system property because it is always set</li>
     * <li>node.name - taken from NodeNamePatternConverter, as it can be set in runtime as hostname when not set in elasticsearch.yml</li>
     * <li>node_and_cluster_id - in json as node.id and cluster.uuid - taken from NodeAndClusterIdConverter and present
     * once clusterStateUpdate is first received</li>
     * <li>message - a json escaped message. Multiline messages will be converted to single line with new line explicitly
     * replaced to \n</li>
     * <li>exceptionAsJson - in json as a stacktrace field. Only present when throwable is passed as a parameter when using a logger.
     * Taken from JsonThrowablePatternConverter</li>
     * </ul>
     */
    private static final String PATTERN = "{" +
        "\"type\": \"${TYPE}\", " +
        "\"timestamp\": \"%d{yyyy-MM-dd'T'HH:mm:ss,SSSZ}\", " +
        "\"level\": \"%p\", " +
        "\"component\": \"%c{1.}\", " +
        "\"cluster.name\": \"${sys:es.logs.cluster_name}\", " +
        "\"node.name\": \"%node_name\", " +
        "%notEmpty{%node_and_cluster_id, } " +
        "\"message\": \"%notEmpty{%enc{%marker}{JSON} }%enc{%.-10000m}{JSON}\" " +
        "%exceptionAsJson " +
        "}%n";

    private final PatternLayout patternLayout;

    protected ESJsonLayout(String typeName, Charset charset) {
        super(charset);
        this.patternLayout = PatternLayout.newBuilder()
                                          .withPattern(pattern(typeName))
                                          .withAlwaysWriteExceptions(false)
                                          .build();
    }

    private String pattern(String type) {
        if (Strings.isEmpty(type)) {
            throw new IllegalArgumentException("layout parameter 'type_name' cannot be empty");
        }
        return PATTERN.replace("${TYPE}", type);
    }

    @PluginFactory
    public static ESJsonLayout createLayout(@PluginAttribute("type_name") String type,
                                            @PluginAttribute(value = "charset", defaultString = "UTF-8") Charset charset) {
        return new ESJsonLayout(type, charset);
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
        final StringBuilder sb = new StringBuilder("ESJsonLayout{");
        sb.append("patternLayout=").append(patternLayout);
        sb.append('}');
        return sb.toString();
    }
}
