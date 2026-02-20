/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.entitlement.runtime.policy.entitlements;

/**
 * Internal entitlement to read code from the jdk.
 *
 * Concretely this means the code can open jrt urls. Since the java
 * runtime images (jrt) are read only, this implicitly only allows
 * reading those urls.
 */
public class ReadJdkImageEntitlement implements Entitlement {}
