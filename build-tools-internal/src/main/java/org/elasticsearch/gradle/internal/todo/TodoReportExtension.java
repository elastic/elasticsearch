/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License, v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.gradle.internal.todo;

import org.gradle.api.provider.ListProperty;

/**
 * Configures which comment markers {@code todoReport} scans for. The default is to-do and fix-me.
 */
public abstract class TodoReportExtension {

    public static final String NAME = "todoReport";

    /** Comment markers to scan for, matched case-insensitively. */
    public abstract ListProperty<String> getMarkers();
}
