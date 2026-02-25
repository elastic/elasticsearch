/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.useragent.api;

import org.elasticsearch.core.Nullable;

/**
 * Parsed user-agent information.
 *
 * @param name       the user-agent name (e.g. "Chrome"); {@code null} when no match
 * @param version    the pre-computed user-agent version string (e.g. "33.0.1750"); {@code null} when major was absent
 * @param os         the operating system name and version; {@code null} when no OS match
 * @param osFull     the pre-computed full OS string (e.g. "Mac OS X 10.9.2"); {@code null} when OS version was absent
 * @param device     the device name and version; {@code null} when no device match
 * @param deviceType the device type (e.g. "Desktop", "Phone"); {@code null} when not extracted
 */
public record Details(
    @Nullable String name,
    @Nullable String version,
    @Nullable VersionedName os,
    @Nullable String osFull,
    @Nullable VersionedName device,
    @Nullable String deviceType
) {

    /**
     * A {@link UserAgentInfoCollector} that accumulates values and builds a {@link Details} record.
     * Follows the pattern of {@code RegisteredDomain.DomainInfo.Factory}.
     */
    public static class Factory implements UserAgentInfoCollector {
        private String name;
        private String version;
        private String osName;
        private String osVersion;
        private String osFull;
        private String deviceName;
        private String deviceVersion;
        private String deviceType;

        @Override
        public void name(String name) {
            this.name = name;
        }

        @Override
        public void version(String version) {
            this.version = version;
        }

        @Override
        public void osName(String osName) {
            this.osName = osName;
        }

        @Override
        public void osVersion(String osVersion) {
            this.osVersion = osVersion;
        }

        @Override
        public void osFull(String osFull) {
            this.osFull = osFull;
        }

        @Override
        public void deviceName(String deviceName) {
            this.deviceName = deviceName;
        }

        @Override
        public void deviceVersion(String deviceVersion) {
            this.deviceVersion = deviceVersion;
        }

        @Override
        public void deviceType(String deviceType) {
            this.deviceType = deviceType;
        }

        /**
         * Builds a {@link Details} record from the accumulated values.
         */
        public Details build() {
            VersionedName os = osName != null ? new VersionedName(osName, osVersion) : null;
            VersionedName device = deviceName != null ? new VersionedName(deviceName, deviceVersion) : null;
            return new Details(name, version, os, osFull, device, deviceType);
        }
    }
}
