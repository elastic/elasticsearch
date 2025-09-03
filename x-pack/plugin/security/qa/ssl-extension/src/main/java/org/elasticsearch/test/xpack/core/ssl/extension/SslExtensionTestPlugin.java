/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.test.xpack.core.ssl.extension;

import org.elasticsearch.plugins.Plugin;

/**
 * Dummy plugin, does not implement any methods, only exists in order to allow {@link TestSslProfile} to be loaded via SPI.
 */
public class SslExtensionTestPlugin extends Plugin {}
