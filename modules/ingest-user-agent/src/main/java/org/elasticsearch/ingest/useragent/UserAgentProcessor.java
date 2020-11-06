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

import org.elasticsearch.common.logging.DeprecationLogger;
import org.elasticsearch.ingest.AbstractProcessor;
import org.elasticsearch.ingest.IngestDocument;
import org.elasticsearch.ingest.Processor;
import org.elasticsearch.ingest.useragent.UserAgentParser.Details;
import org.elasticsearch.ingest.useragent.UserAgentParser.VersionedName;

import java.lang.reflect.Field;
import java.util.Arrays;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;

import static org.elasticsearch.ingest.ConfigurationUtils.newConfigurationException;
import static org.elasticsearch.ingest.ConfigurationUtils.readBooleanProperty;
import static org.elasticsearch.ingest.ConfigurationUtils.readOptionalList;
import static org.elasticsearch.ingest.ConfigurationUtils.readStringProperty;

public class UserAgentProcessor extends AbstractProcessor {

    private static final DeprecationLogger deprecationLogger = DeprecationLogger.getLogger(UserAgentProcessor.class);

    public static final String TYPE = "user_agent";

    private final String field;
    private final String targetField;
    private final Set<Property> properties;
    private final UserAgentParser parser;
    private final boolean ignoreMissing;
    private final boolean useECS;

    public UserAgentProcessor(String tag, String description, String field, String targetField, UserAgentParser parser,
                              Set<Property> properties, boolean ignoreMissing, boolean useECS) {
        super(tag, description);
        this.field = field;
        this.targetField = targetField;
        this.parser = parser;
        this.properties = properties;
        this.ignoreMissing = ignoreMissing;
        this.useECS = useECS;
    }

    boolean isIgnoreMissing() {
        return ignoreMissing;
    }

    @Override
    public IngestDocument execute(IngestDocument ingestDocument) {
        String userAgent = ingestDocument.getFieldValue(field, String.class, ignoreMissing);

        if (userAgent == null && ignoreMissing) {
            return ingestDocument;
        } else if (userAgent == null) {
            throw new IllegalArgumentException("field [" + field + "] is null, cannot parse user-agent.");
        }

        Details uaClient = parser.parse(userAgent);

        Map<String, Object> uaDetails = new HashMap<>();

        if (useECS) {
            // Parse the user agent in the ECS (Elastic Common Schema) format
            for (Property property : this.properties) {
                switch (property) {
                    case ORIGINAL:
                        uaDetails.put("original", userAgent);
                        break;
                    case NAME:
                        if (uaClient.userAgent != null && uaClient.userAgent.name != null) {
                            uaDetails.put("name", uaClient.userAgent.name);
                        } else {
                            uaDetails.put("name", "Other");
                        }
                        break;
                    case VERSION:
                        StringBuilder version = new StringBuilder();
                        if (uaClient.userAgent != null && uaClient.userAgent.major != null) {
                            version.append(uaClient.userAgent.major);
                            if (uaClient.userAgent.minor != null) {
                                version.append(".").append(uaClient.userAgent.minor);
                                if (uaClient.userAgent.patch != null) {
                                    version.append(".").append(uaClient.userAgent.patch);
                                    if (uaClient.userAgent.build != null) {
                                        version.append(".").append(uaClient.userAgent.build);
                                    }
                                }
                            }
                            uaDetails.put("version", version.toString());
                        }
                        break;
                    case OS:
                        if (uaClient.operatingSystem != null) {
                            Map<String, String> osDetails = new HashMap<>(3);
                            if (uaClient.operatingSystem.name != null) {
                                osDetails.put("name", uaClient.operatingSystem.name);
                                StringBuilder sb = new StringBuilder();
                                if (uaClient.operatingSystem.major != null) {
                                    sb.append(uaClient.operatingSystem.major);
                                    if (uaClient.operatingSystem.minor != null) {
                                        sb.append(".").append(uaClient.operatingSystem.minor);
                                        if (uaClient.operatingSystem.patch != null) {
                                            sb.append(".").append(uaClient.operatingSystem.patch);
                                            if (uaClient.operatingSystem.build != null) {
                                                sb.append(".").append(uaClient.operatingSystem.build);
                                            }
                                        }
                                    }
                                    osDetails.put("version", sb.toString());
                                    osDetails.put("full", uaClient.operatingSystem.name + " " + sb.toString());
                                }
                                uaDetails.put("os", osDetails);
                            }
                        }
                        break;
                    case DEVICE:
                        Map<String, String> deviceDetails = new HashMap<>(1);
                        if (uaClient.device != null && uaClient.device.name != null) {
                            deviceDetails.put("name", uaClient.device.name);
                        } else {
                            deviceDetails.put("name", "Other");
                        }
                        uaDetails.put("device", deviceDetails);
                        break;
                }
            }
        } else {
            // Deprecated format, removed in 8.0
            for (Property property : this.properties) {
                switch (property) {
                    case NAME:
                        if (uaClient.userAgent != null && uaClient.userAgent.name != null) {
                            uaDetails.put("name", uaClient.userAgent.name);
                        } else {
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
                        } else {
                            uaDetails.put("os", "Other");
                        }

                        break;
                    case OS_NAME:
                        if (uaClient.operatingSystem != null && uaClient.operatingSystem.name != null) {
                            uaDetails.put("os_name", uaClient.operatingSystem.name);
                        } else {
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
                        } else {
                            uaDetails.put("device", "Other");
                        }
                        break;
                }
            }
        }

        ingestDocument.setFieldValue(targetField, uaDetails);
        return ingestDocument;
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

    public boolean isUseECS() {
        return useECS;
    }

    public static final class Factory implements Processor.Factory {

        private final Map<String, UserAgentParser> userAgentParsers;

        public Factory(Map<String, UserAgentParser> userAgentParsers) {
            this.userAgentParsers = userAgentParsers;
        }

        @Override
        public UserAgentProcessor create(Map<String, Processor.Factory> factories, String processorTag,
                                         String description, Map<String, Object> config) throws Exception {
            String field = readStringProperty(TYPE, processorTag, config, "field");
            String targetField = readStringProperty(TYPE, processorTag, config, "target_field", "user_agent");
            String regexFilename = readStringProperty(TYPE, processorTag, config, "regex_file", IngestUserAgentPlugin.DEFAULT_PARSER_NAME);
            List<String> propertyNames = readOptionalList(TYPE, processorTag, config, "properties");
            boolean ignoreMissing = readBooleanProperty(TYPE, processorTag, config, "ignore_missing", false);
            boolean useECS = readBooleanProperty(TYPE, processorTag, config, "ecs", true);

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

            if (useECS == false) {
                deprecationLogger.deprecate("ecs_false_non_common_schema",
                    "setting [ecs] to false for non-common schema " +
                    "format is deprecated and will be removed in 8.0, set to true or remove to use the non-deprecated format");
            }

            return new UserAgentProcessor(processorTag, description, field, targetField, parser, properties, ignoreMissing, useECS);
        }
    }

    enum Property {

        NAME,
        // Deprecated in 6.7 (superceded by VERSION), to be removed in 8.0
        @Deprecated MAJOR,
        @Deprecated MINOR,
        @Deprecated PATCH,
        OS,
        // Deprecated in 6.7 (superceded by just using OS), to be removed in 8.0
        @Deprecated OS_NAME,
        @Deprecated OS_MAJOR,
        @Deprecated OS_MINOR,
        DEVICE,
        @Deprecated BUILD, // Same deprecated as OS_* above
        ORIGINAL,
        VERSION;

        private static Set<Property> DEPRECATED_PROPERTIES;

        static {
            Set<Property> deprecated = new HashSet<>();
            for (Field field : Property.class.getFields()) {
                if (field.isEnumConstant() && field.isAnnotationPresent(Deprecated.class)) {
                    deprecated.add(valueOf(field.getName()));
                }
            }
            DEPRECATED_PROPERTIES = deprecated;
        }

        public static Property parseProperty(String propertyName) {
            try {
                Property value = valueOf(propertyName.toUpperCase(Locale.ROOT));
                if (DEPRECATED_PROPERTIES.contains(value)) {
                    final String key = "user_agent_processor_property_" + propertyName.replaceAll("[^\\w_]+", "_");
                        deprecationLogger.deprecate(key,
                        "the [{}] property is deprecated for the user-agent processor", propertyName);
                }
                return value;
            }
            catch (IllegalArgumentException e) {
                throw new IllegalArgumentException("illegal property value [" + propertyName + "]. valid values are " +
                        Arrays.toString(EnumSet.allOf(Property.class).toArray()));
            }
        }
    }
}
