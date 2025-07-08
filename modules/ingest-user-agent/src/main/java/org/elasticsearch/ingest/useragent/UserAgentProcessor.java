/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.ingest.useragent;

import org.elasticsearch.cluster.metadata.ProjectId;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.util.Maps;
import org.elasticsearch.core.UpdateForV10;
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
                    if (uaClient.userAgent() != null && uaClient.userAgent().major() != null) {
                        uaDetails.put("version", versionToString(uaClient.userAgent()));
                    }
                    break;
                case OS:
                    if (uaClient.operatingSystem() != null) {
                        Map<String, String> osDetails = Maps.newMapWithExpectedSize(3);
                        if (uaClient.operatingSystem().name() != null) {
                            osDetails.put("name", uaClient.operatingSystem().name());
                            if (uaClient.operatingSystem().major() != null) {
                                String version = versionToString(uaClient.operatingSystem());
                                osDetails.put("version", version);
                                osDetails.put("full", uaClient.operatingSystem().name() + " " + version);
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

    private static String versionToString(final UserAgentParser.VersionedName version) {
        final StringBuilder versionString = new StringBuilder();
        if (Strings.hasLength(version.major())) {
            versionString.append(version.major());
            if (Strings.hasLength(version.minor())) {
                versionString.append(".").append(version.minor());
                if (Strings.hasLength(version.patch())) {
                    versionString.append(".").append(version.patch());
                    if (Strings.hasLength(version.build())) {
                        versionString.append(".").append(version.build());
                    }
                }
            }
        }
        return versionString.toString();
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
            Map<String, Object> config,
            ProjectId projectId
        ) {
            String field = readStringProperty(TYPE, processorTag, config, "field");
            String targetField = readStringProperty(TYPE, processorTag, config, "target_field", "user_agent");
            String regexFilename = readStringProperty(TYPE, processorTag, config, "regex_file", IngestUserAgentPlugin.DEFAULT_PARSER_NAME);
            List<String> propertyNames = readOptionalList(TYPE, processorTag, config, "properties");
            boolean extractDeviceType = readBooleanProperty(TYPE, processorTag, config, "extract_device_type", false);
            boolean ignoreMissing = readBooleanProperty(TYPE, processorTag, config, "ignore_missing", false);

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

    @UpdateForV10(owner = UpdateForV10.Owner.DATA_MANAGEMENT)
    // This can be removed in V10. It's not possible to create an instance with the ecs property in V9, and all instances created by V8 or
    // earlier will have been fixed when upgraded to V9.
    static boolean maybeUpgradeConfig(Map<String, Object> config) {
        // Instances created using ES 8.x (or earlier) may have the 'ecs' config entry.
        // This was ignored in 8.x and is unsupported in 9.0.
        // In 9.x, we should remove it from any existing processors on startup.
        return config.remove("ecs") != null;
    }
}
