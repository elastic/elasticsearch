/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

/**
 * Licensing for xpack.
 *
 * A {@link org.elasticsearch.license.License} is a signed set of json properties that determine what features
 * are available in a running cluster. Licenses are registered through a
 * {@link org.elasticsearch.license.PutLicenseRequest}. This action is handled by the master node, which places
 * the signed license into the cluster state. Each node listens for cluster state updates via the
 * {@link org.elasticsearch.license.LicenseService}, and updates its local copy of the license when it detects
 * changes in the cluster state.
 *
 * The logic for which features are available given the current license is handled by
 * {@link org.elasticsearch.license.XPackLicenseState}, which is updated by the
 * {@link org.elasticsearch.license.LicenseService} when the license changes.
 */
package org.elasticsearch.license;
