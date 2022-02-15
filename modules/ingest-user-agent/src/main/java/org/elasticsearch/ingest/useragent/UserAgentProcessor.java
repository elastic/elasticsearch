/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.ingest.useragent;

import org.elasticsearch.common.logging.DeprecationCategory;
import org.elasticsearch.common.logging.DeprecationLogger;
import org.elasticsearch.common.util.Maps;
import org.elasticsearch.ingest.AbstractProcessor;
import org.elasticsearch.ingest.IngestDocument;
import org.elasticsearch.ingest.Processor;
import org.elasticsearch.ingest.useragent.UserAgentParser.Details;

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

    private static final DeprecationLogger deprecationLogger = DeprecationLogger.getLogger(UserAgentProcessor.class);

    public static final String TYPE = "user_agent";

    private final String field;
    private final String targetField;
    private final Set<Property> properties;
    private final UserAgentParser parser;
    private final boolean extractDeviceType;
    private final boolean ignoreMissing;

    public UserAgentProcessor(
        String tag,
        String description,
        String field,
        String targetField,
        UserAgentParser parser,
        Set<Property> properties,
        boolean extractDeviceType,
        boolean ignoreMissing
    ) {
        super(tag, description);
        this.field = field;
        this.targetField = targetField;
        this.parser = parser;
        this.properties = properties;
        this.extractDeviceType = extractDeviceType;
        this.ignoreMissing = ignoreMissing;
    }

    boolean isExtractDeviceType() {
        return extractDeviceType;
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

        Details uaClient = parser.parse(userAgent, extractDeviceType);

        Map<String, Object> uaDetails = new HashMap<>();

        // Parse the user agent in the ECS (Elastic Common Schema) format
        for (Property property : this.properties) {
            switch (property) {
                case ORIGINAL:
                    uaDetails.put("original", userAgent);
                    break;
                case NAME:
                    if (uaClient.userAgent() != null && uaClient.userAgent().name() != null) {
                        uaDetails.put("name", uaClient.userAgent().name());
                    } else {
                        uaDetails.put("name", "Other");
                    }
                    break;
                case VERSION:
                    StringBuilder version = new StringBuilder();
                    if (uaClient.userAgent() != null && uaClient.userAgent().major() != null) {
                        version.append(uaClient.userAgent().major());
                        if (uaClient.userAgent().minor() != null) {
                            version.append(".").append(uaClient.userAgent().minor());
                            if (uaClient.userAgent().patch() != null) {
                                version.append(".").append(uaClient.userAgent().patch());
                                if (uaClient.userAgent().build() != null) {
                                    version.append(".").append(uaClient.userAgent().build());
                                }
                            }
                        }
                        uaDetails.put("version", version.toString());
                    }
                    break;
                case OS:
                    if (uaClient.operatingSystem() != null) {
                        Map<String, String> osDetails = Maps.newMapWithExpectedSize(3);
                        if (uaClient.operatingSystem().name() != null) {
                            osDetails.put("name", uaClient.operatingSystem().name());
                            StringBuilder sb = new StringBuilder();
                            if (uaClient.operatingSystem().major() != null) {
                                sb.append(uaClient.operatingSystem().major());
                                if (uaClient.operatingSystem().minor() != null) {
                                    sb.append(".").append(uaClient.operatingSystem().minor());
                                    if (uaClient.operatingSystem().patch() != null) {
                                        sb.append(".").append(uaClient.operatingSystem().patch());
                                        if (uaClient.operatingSystem().build() != null) {
                                            sb.append(".").append(uaClient.operatingSystem().build());
                                        }
                                    }
                                }
                                osDetails.put("version", sb.toString());
                                osDetails.put("full", uaClient.operatingSystem().name() + " " + sb.toString());
                            }
                            uaDetails.put("os", osDetails);
                        }
                    }
                    break;
                case DEVICE:
                    Map<String, String> deviceDetails = Maps.newMapWithExpectedSize(1);
                    if (uaClient.device() != null && uaClient.device().name() != null) {
                        deviceDetails.put("name", uaClient.device().name());
                        if (extractDeviceType) {
                            deviceDetails.put("type", uaClient.deviceType());
                        }
                    } else {
                        deviceDetails.put("name", "Other");
                        if (extractDeviceType) {
                            if (uaClient.deviceType() != null) {
                                deviceDetails.put("type", uaClient.deviceType());
                            } else {
                                deviceDetails.put("type", "Other");
                            }
                        }
                    }
                    uaDetails.put("device", deviceDetails);
                    break;
            }
        }

        ingestDocument.setFieldValue(targetField, uaDetails);
        return ingestDocument;
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
        public UserAgentProcessor create(
            Map<String, Processor.Factory> factories,
            String processorTag,
            String description,
            Map<String, Object> config
        ) throws Exception {
            String field = readStringProperty(TYPE, processorTag, config, "field");
            String targetField = readStringProperty(TYPE, processorTag, config, "target_field", "user_agent");
            String regexFilename = readStringProperty(TYPE, processorTag, config, "regex_file", IngestUserAgentPlugin.DEFAULT_PARSER_NAME);
            List<String> propertyNames = readOptionalList(TYPE, processorTag, config, "properties");
            boolean extractDeviceType = readBooleanProperty(TYPE, processorTag, config, "extract_device_type", false);
            boolean ignoreMissing = readBooleanProperty(TYPE, processorTag, config, "ignore_missing", false);
            Object ecsValue = config.remove("ecs");
            if (ecsValue != null) {
                deprecationLogger.warn(
                    DeprecationCategory.SETTINGS,
                    "ingest_useragent_ecs_settings",
                    "setting [ecs] is deprecated as ECS format is the default and only option"
                );
            }

            UserAgentParser parser = userAgentParsers.get(regexFilename);
            if (parser == null) {
                throw newConfigurationException(
                    TYPE,
                    processorTag,
                    "regex_file",
                    "regex file [" + regexFilename + "] doesn't exist (has to exist at node startup)"
                );
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

            return new UserAgentProcessor(
                processorTag,
                description,
                field,
                targetField,
                parser,
                properties,
                extractDeviceType,
                ignoreMissing
            );
        }
    }

    enum Property {

        NAME,
        OS,
        DEVICE,
        ORIGINAL,
        VERSION;

        public static Property parseProperty(String propertyName) {
            try {
                return valueOf(propertyName.toUpperCase(Locale.ROOT));
            } catch (IllegalArgumentException e) {
                throw new IllegalArgumentException(
                    "illegal property value ["
                        + propertyName
                        + "]. valid values are "
                        + Arrays.toString(EnumSet.allOf(Property.class).toArray())
                );
            }
        }
    }
}
