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
 * Marks a {@code String} parameter on a {@link LibrarySpecification @LibrarySpecification} method
 * as UTF-16LE-encoded rather than the implicit UTF-8 default. The framework encodes the argument
 * as UTF-16LE and appends a 2-byte NUL terminator before the native call, matching the {@code wchar_t*} /
 * {@code LPCWSTR} convention used by Windows {@code *W}-suffixed APIs such as
 * {@code GetShortPathNameW} or {@code CreateFileW}.
 *
 * <p>Only valid on {@code String} parameters; applying it to any other parameter type is a
 * compile error. Also only valid on methods whose enclosing {@code @LibrarySpecification} does
 * not list {@link Platform#WINDOWS_X64} in {@link LibrarySpecification#unavailableOn()}, since a
 * wide-string parameter implies the binding must be able to run on Windows.
 */
@Retention(RetentionPolicy.SOURCE)
@Target(ElementType.PARAMETER)
public @interface WideString {
}
