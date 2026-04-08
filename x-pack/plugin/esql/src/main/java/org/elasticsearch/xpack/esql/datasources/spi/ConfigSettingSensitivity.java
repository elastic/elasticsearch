/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasources.spi;

/**
 * Sensitivity classification for a datasource configuration setting.
 */
public enum ConfigSettingSensitivity {
    /** Not sensitive — stored and returned as-is. */
    PLAINTEXT,
    /** Sensitive — masked on GET, encrypted when available. */
    SECRET
}
