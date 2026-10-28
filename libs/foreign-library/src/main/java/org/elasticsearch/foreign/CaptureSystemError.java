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
 * Marks a method on a {@link LibrarySpecification @LibrarySpecification} interface as capturing the
 * operating system's last-error value after the native call, read back via
 * {@link LinkerHelper#systemError()}. POSIX and Windows expose this through different mechanisms —
 * {@code errno} versus Win32 {@code GetLastError} — so the source is selected from the enclosing
 * library's platform availability rather than named on the annotation:
 *
 * <ul>
 *   <li>a library that runs only on Windows (all POSIX platforms listed in
 *       {@link LibrarySpecification#unavailableOn()}) captures {@code GetLastError};</li>
 *   <li>any library that can run on a POSIX platform captures {@code errno}.</li>
 * </ul>
 *
 * <p>Because {@code errno} and {@code GetLastError} are genuinely distinct error channels, the source
 * can only be resolved when the library targets a single platform family. A library available on both
 * Windows and a POSIX platform is a compile error; restrict {@code unavailableOn} to one family.
 */
@Retention(RetentionPolicy.SOURCE)
@Target(ElementType.METHOD)
public @interface CaptureSystemError {
}
