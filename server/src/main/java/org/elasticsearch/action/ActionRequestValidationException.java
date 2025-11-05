/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.action;

import org.elasticsearch.common.ValidationException;

/**
 * Exception thrown when an {@link ActionRequest} fails validation. This exception accumulates
 * multiple validation errors that occurred during request validation, allowing all issues to be
 * reported at once rather than failing on the first error.
 *
 * <p>Validation errors are typically added using
 * {@link org.elasticsearch.action.ValidateActions#addValidationError(String, ActionRequestValidationException)},
 * which builds up a chain of error messages.
 *
 * <p><b>Usage Examples:</b></p>
 * <pre>{@code
 * // In an ActionRequest validate() method
 * @Override
 * public ActionRequestValidationException validate() {
 *     ActionRequestValidationException validationException = null;
 *
 *     if (index == null) {
 *         validationException = ValidateActions.addValidationError(
 *             "index is missing",
 *             validationException
 *         );
 *     }
 *
 *     if (size < 0) {
 *         validationException = ValidateActions.addValidationError(
 *             "size must be positive",
 *             validationException
 *         );
 *     }
 *
 *     return validationException;
 * }
 * }</pre>
 *
 * @see ActionRequest#validate()
 * @see org.elasticsearch.action.ValidateActions#addValidationError(String, ActionRequestValidationException)
 */
public class ActionRequestValidationException extends ValidationException {}
