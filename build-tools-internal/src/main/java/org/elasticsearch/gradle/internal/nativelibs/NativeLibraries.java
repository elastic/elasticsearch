/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */
package org.elasticsearch.gradle.internal.nativelibs;

/** Names shared by the producing and collecting ends of a native library. */
public final class NativeLibraries {

    /** Topic through which a collector finds the projects that build a native library. */
    public static final String TOPIC = "nativeLibraries";

    private NativeLibraries() {}
}
