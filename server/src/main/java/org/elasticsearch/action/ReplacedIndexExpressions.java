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
 * A collection of {@link ReplacedIndexExpression}, keyed by the original expression.
 *
 * <p>An example structure is:</p>
 *
 * <pre>{@code
 * {
 *   "my-index-*": {
 *      {
 *          "original": "my-index-*",
 *          "localExpressions": {
 *              "expressions": ["my-index-000001", "my-index-000002"],
 *              "localIndexResolutionResult": "SUCCESS"
 *          },
 *          "remoteExpressions": ["remote1:my-index-*", "remote2:my-index-*"]
 *      }
 *   }
 * }
 * }</pre>
 */
public record ReplacedIndexExpressions(Map<String, ReplacedIndexExpression> expressions) {}
