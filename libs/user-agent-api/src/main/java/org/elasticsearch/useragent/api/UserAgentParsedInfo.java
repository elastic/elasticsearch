/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.useragent.api;

import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.SequencedMap;

/**
 * Field name constants and field metadata for user-agent parsed information.
 * Used for ES|QL command integration, following the pattern of
 * {@code RegisteredDomain.getRegisteredDomainInfoFields()}.
 */
public final class UserAgentParsedInfo {

    public static final String NAME = "name";
    public static final String VERSION = "version";
    public static final String OS_NAME = "os.name";
    public static final String OS_VERSION = "os.version";
    public static final String OS_FULL = "os.full";
    public static final String DEVICE_NAME = "device.name";
    public static final String DEVICE_TYPE = "device.type";

    private static final SequencedMap<String, Class<?>> USER_AGENT_INFO_FIELDS;

    static {
        LinkedHashMap<String, Class<?>> fields = new LinkedHashMap<>();
        fields.putLast(NAME, String.class);
        fields.putLast(VERSION, String.class);
        fields.putLast(OS_NAME, String.class);
        fields.putLast(OS_VERSION, String.class);
        fields.putLast(OS_FULL, String.class);
        fields.putLast(DEVICE_NAME, String.class);
        fields.putLast(DEVICE_TYPE, String.class);
        USER_AGENT_INFO_FIELDS = Collections.unmodifiableSequencedMap(fields);
    }

    private UserAgentParsedInfo() {}

    /**
     * Returns an ordered map of field names to their types for user-agent parsed information.
     */
    public static SequencedMap<String, Class<?>> getUserAgentInfoFields() {
        return USER_AGENT_INFO_FIELDS;
    }
}
