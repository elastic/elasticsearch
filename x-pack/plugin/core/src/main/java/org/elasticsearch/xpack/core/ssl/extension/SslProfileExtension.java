/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.core.ssl.extension;

import org.elasticsearch.xpack.core.ssl.SslProfile;

import java.util.Set;

import javax.net.ssl.SSLContext;

/**
 * A SPI extension point for defining SSL profiles.
 * Elasticsearch has a standard way of defining SSL Configuration in YAML (see {@link org.elasticsearch.common.ssl.SslConfigurationLoader})
 * and we refer to each of these as either a "profile" or "context" (these are interchangeable, and both are used in the code,
 * however the latter can be confused with {@link SSLContext}).
 * Each profile is loaded on node startup, validated and its source files (PEM certificates, etc) are monitored for changes.
 * This extension point makes it easy for modules and plugins to define new profiles.
 */
public interface SslProfileExtension {

    /**
     * @return the setting prefixes that this extension supports. For example {@code xpack.foo.ssl}
     * It must end in {@code ".ssl"}
     */
    Set<String> getSettingPrefixes();

    /**
     * Called after each SSL profile has been loaded and validated
     */
    void applyProfile(String prefix, SslProfile profile);

}
