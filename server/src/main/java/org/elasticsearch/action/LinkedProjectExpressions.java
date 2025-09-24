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
 * // TODO MP update docs up to "An example structure is"
 * This class allows capturing context about index expression replacements performed on an {@link IndicesRequest.Replaceable} during
 * index resolution, in particular the results of local resolution, and the remote (unresolved) expressions if any.
 * <p>
 * The replacements are separated into local and remote expressions.
 * For local expressions, the class allows recording local index resolution results along with failure info.
 * For remote expressions, only the expressions are recorded.
 *
 * <p>An example structure is:</p>
 *
 * <pre>{@code
 * {
 *   "P*:my-index-*": {
 *     "expressions": ["my-index-000001", "my-index-000002"],
 *     "localIndexResolutionResult": "SUCCESS"
 *   }
 * }
 * }</pre>
 *
 * @param resolvedExpressions a map keyed by the original expression and having as value the remote resolution for that expression.
 */
public record LinkedProjectExpressions(Map<String, ResolvedIndexExpression.LocalExpressions> resolvedExpressions) {

}
