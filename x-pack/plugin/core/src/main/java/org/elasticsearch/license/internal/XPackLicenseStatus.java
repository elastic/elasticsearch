/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.license.internal;

import org.elasticsearch.license.License;

/**
 * A wrapper for the license mode, state, and expiration date, to allow atomically swapping.
 * @param mode The current "mode" of the license (ie license type).
 * @param active True if the license is active, or false if it is expired.
 * @param expiryWarning A warning to be emitted on license checks about the license expiring soon.
 */
public record XPackLicenseStatus(License.OperationMode mode, boolean active, String expiryWarning) {

}
