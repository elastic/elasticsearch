/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */
package org.elasticsearch.gradle.internal.nativelibs;

/** Names shared by the build logic that builds native libraries and the build logic that uses them. */
public final class NativeLibraries {

    /**
     * Topic for projects that build native libraries.
     * Every project applying {@link NativeLibraryBuildPlugin} registers to it, and
     * {@link NativeLibraryCollectionPlugin} uses it to find them.
     */
    public static final String TOPIC = "nativeLibraries";

    /**
     * Value of Gradle artifact type attribute for the directory containing a project's native
     * libraries. NativeLibraryBuildPlugin sets it on that directory, and
     * NativeLibraryPathExtension#libraryDirectoriesOf uses it to select those directories.
     */
    public static final String ARTIFACT_TYPE = "es-native-libraries";

    /**
     * Value of Gradle's {@code LibraryElements} attribute for the directory containing a
     * project's native libraries. We want to set the attribute to something that is different
     * from {@code jar} so Gradle keeps the native libraries directory off runtime classpaths.
     */
    public static final String LIBRARY_ELEMENTS = "native-libraries";

    /** System property listing the directories the JVM uses to load native libraries. */
    public static final String PATH_PROPERTY = "es.nativelibs.path";

    private NativeLibraries() {}
}
