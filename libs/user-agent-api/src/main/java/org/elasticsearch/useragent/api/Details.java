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
) {}
