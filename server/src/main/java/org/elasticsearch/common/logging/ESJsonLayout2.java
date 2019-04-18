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
import org.apache.logging.log4j.core.config.plugins.*;
import org.apache.logging.log4j.core.layout.AbstractStringLayout;
import org.apache.logging.log4j.core.layout.ByteBufferDestination;
import org.apache.logging.log4j.core.layout.PatternLayout;
import org.apache.logging.log4j.core.util.KeyValuePair;
import org.elasticsearch.common.Strings;

import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.util.LinkedHashMap;
import java.util.Map;

/**
 * Formats log events as strings in a json format.
 * <p>
 * The class is wrapping the {@link PatternLayout} with a pattern to format into json. This gives more flexibility and control over how the
 * log messages are formatted in {@link org.apache.logging.log4j.core.layout.JsonLayout}
 */
@Plugin(name = "ESJsonLayout2", category = Node.CATEGORY, elementType = Layout.ELEMENT_TYPE, printObject = true)
public class ESJsonLayout2 extends AbstractStringLayout {
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
    private final PatternLayout patternLayout;


    protected ESJsonLayout2(String typeName, Charset charset, KeyValuePair[] additionalFields, String[] esmessagefields) {
        super(charset);
        this.patternLayout = PatternLayout.newBuilder()
            .withPattern(pattern(typeName, additionalFields,esmessagefields))
            .withAlwaysWriteExceptions(false)
            .build();
    }

    private String pattern(String type, KeyValuePair[] additionalFields, String[] esmessagefields) {
        if (Strings.isEmpty(type)) {
            throw new IllegalArgumentException("layout parameter 'type_name' cannot be empty");
        }
//        Map<String,Object> map = new LinkedHashMap<>();
//        map.put("type",type);
//        map.put("timestamp","%d{yyyy-MM-dd'T'HH:mm:ss,SSSZ}");
//        map.put("level","%p");
//        map.put("component","%c{1.}");
//        map.put("cluster.name","${sys:es.logs.cluster_name}");
//        map.put("node.name","%node_name");
//        map.put("message","%notEmpty{%enc{%marker}{JSON} }%enc{%.-10000m}{JSON}");
//
//        for(String key : esmessagefields){
//            map.put(key,"%ESMessageField{"+key+"}");
//        }
//        for (KeyValuePair keyValuePair : additionalFields) {
//            map.put(keyValuePair.getKey(),asJson(keyValuePair.getValue()));
//        }
//
//
        StringBuilder sb = new StringBuilder();
        sb.append("{");
        sb.append(jsonKey("type") + inQuotes(type));
        sb.append(", " + jsonKey("timestamp") + inQuotes("%d{yyyy-MM-dd'T'HH:mm:ss,SSSZ}"));
        sb.append(", " + jsonKey("level") + inQuotes("%p"));
        sb.append(", " + jsonKey("component") + inQuotes("%c{1.}"));
        sb.append(", " + jsonKey("cluster.name") + inQuotes("${sys:es.logs.cluster_name}"));
        sb.append(", " + jsonKey("node.name") + inQuotes("%node_name"));
        sb.append(", " + jsonKey("message") + inQuotes("%notEmpty{%enc{%marker}{JSON} }%enc{%.-10000m}{JSON}"));
        sb.append(notEmpty(", %node_and_cluster_id "));

        for (String key : esmessagefields) {
            sb.append(", " +jsonKey(key) + inQuotes("%ESMessageField{"+key+"}"));
        }

        for (KeyValuePair keyValuePair : additionalFields) {
            sb.append(", " +jsonKey(keyValuePair.getKey()) + inQuotes(asJson(keyValuePair.getValue())));
        }
        sb.append("%exceptionAsJson ");
        sb.append("}");
        sb.append(System.lineSeparator());

        return sb.toString();
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

    private String asJson(String s) {
        return "%enc{" + s + "}{JSON}";
    }

    @PluginFactory
    public static ESJsonLayout2 createLayout(String type,
                                             Charset charset,
                                             KeyValuePair[] additionalFields,
                                             String[] esmessagefields) {
        return new ESJsonLayout2(type, charset, additionalFields,esmessagefields);
    }

    public static class Builder<B extends ESJsonLayout2.Builder<B>> extends AbstractStringLayout.Builder<B>
        implements org.apache.logging.log4j.core.util.Builder<ESJsonLayout2> {

        @PluginAttribute("type_name")
        String type;

        @PluginAttribute(value = "charset", defaultString = "UTF-8")
        Charset charset;

        @PluginAttribute("esmessagefields")
        private String esmessagefields;

        @PluginElement("AdditionalField")
        private KeyValuePair[] additionalFields;



        public Builder() {
            super();
            setCharset(StandardCharsets.UTF_8);
        }

        @Override
        public ESJsonLayout2 build() {
            String[] split = Strings.isNullOrEmpty(esmessagefields) ?  new String[]{} : esmessagefields.split(",");
            return ESJsonLayout2.createLayout(type, charset, additionalFields, split);
        }

        public Charset getCharset() {
            return charset;
        }

        public B setCharset(final Charset type) {
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

        public KeyValuePair[] getAdditionalFields() {
            return additionalFields;
        }

        public B setAdditionalFields(KeyValuePair[] additionalFields) {
            this.additionalFields = additionalFields;
            return asBuilder();
        }

        public String getEsmessagefields() {
            return esmessagefields;
        }

        public B setEsmessagefields(String esmessagefields) {
            this.esmessagefields = esmessagefields;
            return asBuilder();
        }
    }

    @PluginBuilderFactory
    public static <B extends ESJsonLayout2.Builder<B>> B newBuilder() {
        return new ESJsonLayout2.Builder<B>().asBuilder();
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
