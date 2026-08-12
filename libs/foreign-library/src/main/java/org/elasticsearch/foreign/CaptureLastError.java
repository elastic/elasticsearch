/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.foreign;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * Marks a method on a {@link LibrarySpecification @LibrarySpecification} interface as capturing
 * the Win32 {@code GetLastError} value after the native call. The generated implementation will
 * use {@link LinkerHelper#LAST_ERROR_STATE} as the shared last-error capture buffer.
 *
 * <p>This annotation is mutually exclusive with {@link CaptureErrno} on the same method: POSIX
 * {@code errno} and Win32 {@code GetLastError} are set by disjoint sets of native functions, so a
 * single binding never needs both.
 */
@Retention(RetentionPolicy.SOURCE)
@Target(ElementType.METHOD)
public @interface CaptureLastError {
}
