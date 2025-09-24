/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.action;

import java.util.Map;

/**
 * A set of {@link LinkedProjectExpressions}, keyed by the project alias.
 *
 * <p>An example structure is:</p>
 *
 * <pre>{@code
 * {
 *   "P1": {
 *      "P1:my-index-*": { //example qualified
 *             "expressions": ["my-index-000001", "my-index-000002"],
 *             "localIndexResolutionResult": "SUCCESS"
 *       },
 *       "my-metrics-*": { //example flat
 *             "expressions": ["my-metrics-000001", "my-metrics-000002"],
 *             "localIndexResolutionResult": "SUCCESS"
 *       }
 *   },
 *   "P2": {
 *      "my-index-*": {
 *          "expressions": ["my-index-000001", "my-index-000002"],
 *          "localIndexResolutionResult": "SUCCESS"
 *      }
 *   }
 * }
 * }</pre>
 */
public record RemoteIndexExpressions(Map<String, LinkedProjectExpressions> expressions) {}
