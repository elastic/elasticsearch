/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

/**
 * Telemetry <strong>export</strong> for the {@code apm} module: how metrics and traces leave the Elasticsearch
 * process after being recorded. (This is distinct from <em>instrumentation</em>, which uses the OpenTelemetry API
 * in application code to create spans and instruments.)
 *
 * <p>Subpackages split the two export paths used by this module:
 * <ul>
 *   <li>{@link org.elasticsearch.telemetry.apm.internal.export.agent} — export via the Elasticsearch APM Java agent</li>
 *   <li>{@link org.elasticsearch.telemetry.apm.internal.export.otelsdk} — export via the OpenTelemetry SDK</li>
 * </ul>
 */
package org.elasticsearch.telemetry.apm.internal.export;
