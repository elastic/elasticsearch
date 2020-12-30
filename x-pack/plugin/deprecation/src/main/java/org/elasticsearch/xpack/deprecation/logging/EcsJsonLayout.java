/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.deprecation.logging;

import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

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
import org.elasticsearch.common.logging.ESJsonLayout;

/**
 * This is in essense a fork of {@link ESJsonLayout}, with tweaks to align the output more closely
 * with ECS. This will be removed in the next major release of ES.
 */
@Plugin(name = "EcsJsonLayout", category = Node.CATEGORY, elementType = Layout.ELEMENT_TYPE, printObject = true)
public class EcsJsonLayout extends AbstractStringLayout {
    private static final String ECS_VERSION = "1.6";

    private final PatternLayout patternLayout;

    protected EcsJsonLayout(String typeName, Charset charset, String[] esmessagefields) {
        super(charset);
        this.patternLayout = PatternLayout.newBuilder()
                                          .withPattern(pattern(typeName, esmessagefields))
                                          .withAlwaysWriteExceptions(false)
                                          .build();
    }

    protected String pattern(String type, String[] esMessageFields) {
        if (Strings.isEmpty(type)) {
            throw new IllegalArgumentException("layout parameter 'type_name' cannot be empty");
        }
        Map<String, Object> map = new LinkedHashMap<>();
        map.put("type", inQuotes(type));
        map.put("@timestamp", inQuotes("%d{yyyy-MM-dd'T'HH:mm:ss,SSSZZ}"));
        map.put("level", inQuotes("%p"));
        map.put("component", inQuotes("%c"));
        map.put("cluster.name", inQuotes("${sys:es.logs.cluster_name}"));
        map.put("node.name", inQuotes("%node_name"));
        map.put("message", inQuotes("%notEmpty{%enc{%marker}{JSON} }%enc{%.-10000m}{JSON}"));
        map.put("data_stream.type", inQuotes("logs"));
        map.put("data_stream.dataset", inQuotes("deprecation.elasticsearch"));
        map.put("data_stream.namespace", inQuotes("default"));
        map.put("ecs.version", inQuotes(ECS_VERSION));

        for (String key : esMessageFields) {
            map.put(key, inQuotes("%ESMessageField{" + key + "}"));
        }
        return createPattern(map, Stream.of(esMessageFields).collect(Collectors.toSet()));
    }


    protected String createPattern(Map<String, Object> map, Set<String> esMessageFields) {
        StringBuilder sb = new StringBuilder();
        sb.append("{");
        String separator = "";
        for (Map.Entry<String, Object> entry : map.entrySet()) {

            if (esMessageFields.contains(entry.getKey())) {
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

    protected String inQuotes(String s) {
        return "\"" + s + "\"";
    }

    @PluginFactory
    public static EcsJsonLayout createLayout(String type,
                                             Charset charset,
                                             String[] esmessagefields) {
        return new EcsJsonLayout(type, charset, esmessagefields);
    }

    PatternLayout getPatternLayout() {
        return patternLayout;
    }

    public static class Builder<B extends EcsJsonLayout.Builder<B>> extends AbstractStringLayout.Builder<B>
        implements org.apache.logging.log4j.core.util.Builder<EcsJsonLayout> {

        @PluginAttribute("type_name")
        String type;

        @PluginAttribute(value = "charset", defaultString = "UTF-8")
        Charset charset;

        @PluginAttribute("esmessagefields")
        private String esMessageFields;

        public Builder() {
            setCharset(StandardCharsets.UTF_8);
        }

        @Override
        public EcsJsonLayout build() {
            String[] split = Strings.isNullOrEmpty(esMessageFields) ? new String[]{} : esMessageFields.split(",");
            return EcsJsonLayout.createLayout(type, charset, split);
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

        public String getESMessageFields() {
            return esMessageFields;
        }

        public B setESMessageFields(String esmessagefields) {
            this.esMessageFields = esmessagefields;
            return asBuilder();
        }
    }

    @PluginBuilderFactory
    public static <B extends EcsJsonLayout.Builder<B>> B newBuilder() {
        return new EcsJsonLayout.Builder<B>().asBuilder();
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
        return "EcsJsonLayout{patternLayout=" + patternLayout + '}';
    }
}
