/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

/**
 * Contains classes that need to be used directly from instrumented methods.
 * It's a minimal shim that is patched into the {@code java.base} module so that it is callable from the class library methods instrumented
 * by the agent. The shim retains a {@link org.elasticsearch.entitlement.bridge.InstrumentationRegistry} instance (inside its
 * {@link org.elasticsearch.entitlement.bridge.InstrumentationRegistryHandle} holder) and forwards the entitlement checks to the main
 * library, that exists in the system classloader.
 * {@link org.elasticsearch.entitlement.bridge.InstrumentationRegistry} holds all the entitlements check definitions, one for each
 * instrumented method.
 */
package org.elasticsearch.entitlement.bridge;
