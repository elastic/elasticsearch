/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.ml;

/**
 * In order to allow enhancements that require additions to the ML custom cluster state to be made in minor versions,
 * when we parse our metadata from persisted cluster state we ignore unknown fields.  However, we don't want to be
 * lenient when parsing config as this would mean user mistakes could go undetected.  Therefore, for all JSON objects
 * that are used in both custom cluster state and config we have two parsers, one tolerant of unknown fields (for
 * parsing cluster state) and one strict (for parsing config).  This class enumerates the two options.
 */
public enum MlParserType {

    METADATA, CONFIG;

}
