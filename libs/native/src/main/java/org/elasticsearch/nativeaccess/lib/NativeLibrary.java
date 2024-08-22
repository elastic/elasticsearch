/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.nativeaccess.lib;

/** A marker interface for libraries that can be loaded by {@link org.elasticsearch.nativeaccess.lib.NativeLibraryProvider} */
public sealed interface NativeLibrary permits JavaLibrary, PosixCLibrary, LinuxCLibrary, MacCLibrary, Kernel32Library, VectorLibrary,
    ZstdLibrary {}
