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
import org.apache.logging.log4j.core.config.plugins.PluginBuilderFactory;
import org.apache.logging.log4j.core.config.plugins.PluginFactory;
import org.apache.logging.log4j.core.layout.AbstractStringLayout;
import org.apache.logging.log4j.core.layout.ByteBufferDestination;
import org.apache.logging.log4j.core.layout.PatternLayout;
import org.elasticsearch.common.Strings;

import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Set;

/**
 * Formats log events as strings in a json format.
 * <p>
 * The class is wrapping the {@link PatternLayout} with a pattern to format into json. This gives more flexibility and control over how the
 * log messages are formatted in {@link org.apache.logging.log4j.core.layout.JsonLayout}
 * There are fields which are always present in the log line:
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
 * <p>
 * It is possible to add more field by  using {@link ESLogMessage#with} method which allow adding key value pairs
 * or override field with <code>overrideFields</code>
 * <code>appender.logger.layout.overrideFields=message</code>.
 * In the example above the pattern will be <code>... "message":  %OverrideField{message} ...</code>
 * the message passed to a logger will be overridden with a value from %OverrideField{message}
 * Once an appender is defined to be overriding a field, all the log events should contain this field.
 * <p>
 * The value taken from ESLogMessage has to be a simple escaped JSON value.
 */
@Plugin(name = "ESJsonLayout", category = Node.CATEGORY, elementType = Layout.ELEMENT_TYPE, printObject = true)
public class ESJsonLayout extends AbstractStringLayout {

    private final PatternLayout patternLayout;
    private String overridenFields;

    protected ESJsonLayout(String typeName, Charset charset, String[] overrideFields) {
        super(charset);
        this.overridenFields = String.join(",",overrideFields);
        this.patternLayout = PatternLayout.newBuilder()
                                          .withPattern(pattern(typeName, overrideFields))
                                          .withAlwaysWriteExceptions(false)
                                          .build();
    }

    private String pattern(String type, String[] overrideFields) {
        if (Strings.isEmpty(type)) {
            throw new IllegalArgumentException("layout parameter 'type_name' cannot be empty");
        }
        Map<String, Object> map = new LinkedHashMap<>();
        map.put("type", inQuotes(type));
        map.put("timestamp", inQuotes("%d{yyyy-MM-dd'T'HH:mm:ss,SSSZZ}"));
        map.put("level", inQuotes("%p"));
        map.put("component", inQuotes("%c{1.}"));
        map.put("cluster.name", inQuotes("${sys:es.logs.cluster_name}"));
        map.put("node.name", inQuotes("%node_name"));
        map.put("message", inQuotes("%notEmpty{%enc{%marker}{JSON} }%enc{%.-10000m}{JSON}"));


        // overridden fields are expected to be present in a log message
        for (String key : overrideFields) {
            map.remove(key);
        }

        return createPattern(map, Set.of(overrideFields));
    }


    private String createPattern(Map<String, Object> map, Set<String> overrideFields) {
        StringBuilder sb = new StringBuilder();
        sb.append("{");
        String separator = "";
        for (Map.Entry<String, Object> entry : map.entrySet()) {

            if (overrideFields.contains(entry.getKey())) {
                sb.append("%notEmpty{");
                sb.append(separator);
                appendField(sb, entry);
                sb.append("}");
            } else {
                sb.append(separator);
                appendField(sb, entry);
            }

            separator = ", ";
        }
        sb.append(notEmpty(", %node_and_cluster_id "));
        sb.append(notEmpty(", %CustomMapFields{"+overridenFields+"} "));
        sb.append("%exceptionAsJson ");
        sb.append("}");
        sb.append(System.lineSeparator());

        return sb.toString();
    }

    private void appendField(StringBuilder sb, Map.Entry<String, Object> entry) {
        sb.append(jsonKey(entry.getKey()));
        sb.append(entry.getValue().toString());
    }

    private String notEmpty(String value) {
        return "%notEmpty{" + value + "}";
    }

    private CharSequence jsonKey(String s) {
        return inQuotes(s) + ": ";
    }

    private String inQuotes(String s) {
        return "\"" + s + "\"";
    }

    @PluginFactory
    public static ESJsonLayout createLayout(String type,
                                            Charset charset,
                                            String[] overrideFields) {
        return new ESJsonLayout(type, charset, overrideFields);
    }

    PatternLayout getPatternLayout() {
        return patternLayout;
    }

    public static class Builder<B extends ESJsonLayout.Builder<B>> extends AbstractStringLayout.Builder<B>
        implements org.apache.logging.log4j.core.util.Builder<ESJsonLayout> {

        @PluginAttribute("type_name")
        String type;

        @PluginAttribute(value = "charset", defaultString = "UTF-8")
        Charset charset;

        @PluginAttribute("overrideFields")
        private String overrideFields;

        public Builder() {
            setCharset(StandardCharsets.UTF_8);
        }

        @Override
        public ESJsonLayout build() {
            String[] split = Strings.isNullOrEmpty(overrideFields) ? new String[]{} : overrideFields.split(",");
            return ESJsonLayout.createLayout(type, charset, split);
        }

        public Charset getCharset() {
            return charset;
        }

        public B setCharset(final Charset charset) {
            this.charset = charset;
            return asBuilder();
        }

        public String getType() {
            return type;
        }

        public B setType(final String type) {
            this.type = type;
            return asBuilder();
        }

        public String getOverrideFields() {
            return overrideFields;
        }

        public B setOverrideFields(String overrideFields) {
            this.overrideFields = overrideFields;
            return asBuilder();
        }
    }

    @PluginBuilderFactory
    public static <B extends ESJsonLayout.Builder<B>> B newBuilder() {
        return new ESJsonLayout.Builder<B>().asBuilder();
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
