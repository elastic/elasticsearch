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
 * by the agent. The shim retains a {@link org.elasticsearch.entitlement.bridge.EntitlementChecker} instance (inside its
 * {@link org.elasticsearch.entitlement.bridge.EntitlementCheckerHandle} holder) and forwards the entitlement checks to the main library,
 * that exists in the system classloader.
 * {@link org.elasticsearch.entitlement.bridge.EntitlementChecker} holds all the entitlements check definitions, one for each instrumented
 * method.
 * <p>
 * In order to work across multiple Java versions, this project uses multi-release jars via the {@code mrjar} plugin, which makes it is
 * possible to specify classes for specific Java versions in specific {@code src} folders (e.g. {@code main23} for classes available to
 * Java 23+).
 * All the versioned Java classes are merged into the bridge jar. Therefore, we must prefix the class name
 * with the version, e.g. {@code Java23EntitlementCheckerHandle} and {@code Java23EntitlementChecker}.
 * </p>
 */
package org.elasticsearch.entitlement.bridge;
