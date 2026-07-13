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
 * A name with an optional pre-computed version string.
 *
 * @param name    the name (e.g. OS name, device name); never {@code null}
 * @param version the pre-computed version string (e.g. "10.9.2"); {@code null} when the version major component was absent
 */
public record VersionedName(String name, @Nullable String version) {}
