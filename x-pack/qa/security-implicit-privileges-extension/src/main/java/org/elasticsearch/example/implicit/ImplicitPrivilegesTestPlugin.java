/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.example.implicit;

import org.elasticsearch.plugins.Plugin;

/**
 * Marker plugin that exists only so the test cluster has a jar to load. The actual extension
 * point — {@link ImplicitPrivilegesTestExtension} — is discovered via the
 * {@code org.elasticsearch.xpack.core.security.SecurityExtension} SPI file in this jar's
 * {@code META-INF/services} directory.
 */
public class ImplicitPrivilegesTestPlugin extends Plugin {}
