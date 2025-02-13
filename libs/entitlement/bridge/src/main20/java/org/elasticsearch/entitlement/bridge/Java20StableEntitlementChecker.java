/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.entitlement.bridge;

import java.lang.foreign.FunctionDescriptor;
import java.lang.foreign.Linker;
import java.nio.file.LinkOption;
import java.nio.file.Path;
import java.nio.file.spi.FileSystemProvider;

/**
 * Interface with Java20 "stable" functions and types.
 * It inherits from the previous "stable" interface, in this case {@link EntitlementChecker} as there is no Java19-specific stable
 * API to instrument.
 */
public interface Java20StableEntitlementChecker extends EntitlementChecker {

    /**
     * This overload of downcallHandle has its final form starting from Java 20
     * See docs: https://docs.oracle.com/en/java/javase/20/docs/api/java.base/java/lang/foreign/Linker.html#downcallHandle(java.lang.foreign.FunctionDescriptor,java.lang.foreign.Linker.Option...)
     *
     * Its only allowed implementation is in AbstractLinker:
     * https://github.com/openjdk/jdk20u/blob/9ced461a4d8cb2ecfe2d6a74ec218ec589dcd617/src/java.base/share/classes/jdk/internal/foreign/abi/AbstractLinker.java#L52
     */
    void check$jdk_internal_foreign_abi_AbstractLinker$downcallHandle(
        Class<?> callerClass,
        Linker that,
        FunctionDescriptor function,
        Linker.Option... options
    );

    void checkReadAttributesIfExists(Class<?> callerClass, FileSystemProvider that, Path path, Class<?> type, LinkOption... options);

    void checkExists(Class<?> callerClass, FileSystemProvider that, Path path, LinkOption... options);
}
