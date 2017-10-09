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

package org.elasticsearch.ingest.useragent;

import org.elasticsearch.ingest.AbstractProcessor;
import org.elasticsearch.ingest.IngestDocument;
import org.elasticsearch.ingest.Processor;
import org.elasticsearch.ingest.useragent.UserAgentParser.Details;
import org.elasticsearch.ingest.useragent.UserAgentParser.VersionedName;

import java.util.Arrays;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;

import static org.elasticsearch.ingest.ConfigurationUtils.newConfigurationException;
import static org.elasticsearch.ingest.ConfigurationUtils.readBooleanProperty;
import static org.elasticsearch.ingest.ConfigurationUtils.readOptionalList;
import static org.elasticsearch.ingest.ConfigurationUtils.readStringProperty;

public class UserAgentProcessor extends AbstractProcessor {

    public static final String TYPE = "user_agent";

    private final String field;
    private final String targetField;
    private final Set<Property> properties;
    private final UserAgentParser parser;
    private final boolean ignoreMissing;

    public UserAgentProcessor(String tag, String field, String targetField, UserAgentParser parser, Set<Property> properties,
                              boolean ignoreMissing) {
        super(tag);
        this.field = field;
        this.targetField = targetField;
        this.parser = parser;
        this.properties = properties;
        this.ignoreMissing = ignoreMissing;
    }

    boolean isIgnoreMissing() {
        return ignoreMissing;
    }

    @Override
    public void execute(IngestDocument ingestDocument) throws Exception {
        String userAgent = ingestDocument.getFieldValue(field, String.class, ignoreMissing);

        if (userAgent == null && ignoreMissing) {
            return;
        } else if (userAgent == null) {
            throw new IllegalArgumentException("field [" + field + "] is null, cannot parse user-agent.");
        }

        Details uaClient = parser.parse(userAgent);

        Map<String, Object> uaDetails = new HashMap<>();
        for (Property property : this.properties) {
            switch (property) {
                case NAME:
                    if (uaClient.userAgent != null && uaClient.userAgent.name != null) {
                        uaDetails.put("name", uaClient.userAgent.name);
                    }
                    else {
                        uaDetails.put("name", "Other");
                    }
                    break;
                case MAJOR:
                    if (uaClient.userAgent != null && uaClient.userAgent.major != null) {
                        uaDetails.put("major", uaClient.userAgent.major);
                    }
                    break;
                case MINOR:
                    if (uaClient.userAgent != null && uaClient.userAgent.minor != null) {
                        uaDetails.put("minor", uaClient.userAgent.minor);
                    }
                    break;
                case PATCH:
                    if (uaClient.userAgent != null && uaClient.userAgent.patch != null) {
                        uaDetails.put("patch", uaClient.userAgent.patch);
                    }
                    break;
                case BUILD:
                    if (uaClient.userAgent != null && uaClient.userAgent.build != null) {
                        uaDetails.put("build", uaClient.userAgent.build);
                    }
                    break;
                case OS:
                    if (uaClient.operatingSystem != null) {
                        uaDetails.put("os", buildFullOSName(uaClient.operatingSystem));
                    }
                    else {
                        uaDetails.put("os", "Other");
                    }

                    break;
                case OS_NAME:
                    if (uaClient.operatingSystem != null && uaClient.operatingSystem.name != null) {
                        uaDetails.put("os_name", uaClient.operatingSystem.name);
                    }
                    else {
                        uaDetails.put("os_name", "Other");
                    }
                    break;
                case OS_MAJOR:
                    if (uaClient.operatingSystem != null && uaClient.operatingSystem.major != null) {
                        uaDetails.put("os_major", uaClient.operatingSystem.major);
                    }
                    break;
                case OS_MINOR:
                    if (uaClient.operatingSystem != null && uaClient.operatingSystem.minor != null) {
                        uaDetails.put("os_minor", uaClient.operatingSystem.minor);
                    }
                    break;
                case DEVICE:
                    if (uaClient.device != null && uaClient.device.name != null) {
                        uaDetails.put("device", uaClient.device.name);
                    }
                    else {
                        uaDetails.put("device", "Other");
                    }
                    break;
            }
        }

        ingestDocument.setFieldValue(targetField, uaDetails);
    }

    /** To maintain compatibility with logstash-filter-useragent */
    private String buildFullOSName(VersionedName operatingSystem) {
        if (operatingSystem == null || operatingSystem.name == null) {
            return null;
        }

        StringBuilder sb = new StringBuilder(operatingSystem.name);

        if (operatingSystem.major != null) {
            sb.append(" ");
            sb.append(operatingSystem.major);

            if (operatingSystem.minor != null) {
                sb.append(".");
                sb.append(operatingSystem.minor);

                if (operatingSystem.patch != null) {
                    sb.append(".");
                    sb.append(operatingSystem.patch);

                    if (operatingSystem.build != null) {
                        sb.append(".");
                        sb.append(operatingSystem.build);
                    }
                }
            }
        }

        return sb.toString();
    }

    @Override
    public String getType() {
        return TYPE;
    }

    String getField() {
        return field;
    }

    String getTargetField() {
        return targetField;
    }

    Set<Property> getProperties() {
        return properties;
    }

    UserAgentParser getUaParser() {
        return parser;
    }

    public static final class Factory implements Processor.Factory {

        private final Map<String, UserAgentParser> userAgentParsers;

        public Factory(Map<String, UserAgentParser> userAgentParsers) {
            this.userAgentParsers = userAgentParsers;
        }

        @Override
        public UserAgentProcessor create(Map<String, Processor.Factory> factories, String processorTag,
                                         Map<String, Object> config) throws Exception {
            String field = readStringProperty(TYPE, processorTag, config, "field");
            String targetField = readStringProperty(TYPE, processorTag, config, "target_field", "user_agent");
            String regexFilename = readStringProperty(TYPE, processorTag, config, "regex_file", IngestUserAgentPlugin.DEFAULT_PARSER_NAME);
            List<String> propertyNames = readOptionalList(TYPE, processorTag, config, "properties");
            boolean ignoreMissing = readBooleanProperty(TYPE, processorTag, config, "ignore_missing", false);

            UserAgentParser parser = userAgentParsers.get(regexFilename);
            if (parser == null) {
                throw newConfigurationException(TYPE, processorTag,
                        "regex_file", "regex file [" + regexFilename + "] doesn't exist (has to exist at node startup)");
            }

            final Set<Property> properties;
            if (propertyNames != null) {
                properties = EnumSet.noneOf(Property.class);
                for (String fieldName : propertyNames) {
                    try {
                        properties.add(Property.parseProperty(fieldName));
                    } catch (IllegalArgumentException e) {
                        throw newConfigurationException(TYPE, processorTag, "properties", e.getMessage());
                    }
                }
            } else {
                properties = EnumSet.allOf(Property.class);
            }

            return new UserAgentProcessor(processorTag, field, targetField, parser, properties, ignoreMissing);
        }
    }

    enum Property {

        NAME, MAJOR, MINOR, PATCH, OS, OS_NAME, OS_MAJOR, OS_MINOR, DEVICE, BUILD;

        public static Property parseProperty(String propertyName) {
            try {
                return valueOf(propertyName.toUpperCase(Locale.ROOT));
            }
            catch (IllegalArgumentException e) {
                throw new IllegalArgumentException("illegal property value [" + propertyName + "]. valid values are " +
                        Arrays.toString(EnumSet.allOf(Property.class).toArray()));
            }
        }
    }
}
