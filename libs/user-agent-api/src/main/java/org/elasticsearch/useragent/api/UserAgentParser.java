/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.useragent.api;

/**
 * Parses user-agent strings into structured information.
 */
public interface UserAgentParser {

    /**
     * Parses the given user-agent string and returns a {@link Details} record.
     *
     * @param agentString       the user-agent header value to parse
     * @param extractDeviceType whether to extract the device type
     * @return the parsed details
     */
    Details parseUserAgentInfo(String agentString, boolean extractDeviceType);
}
