/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

/**
 * <p>
 * Contains classes that need to be used directly from instrumented methods.
 * It's a minimal shim that is patched into the {@code java.base} module so that it is callable from the class library methods instrumented
 * by the agent. The shim retains a {@link org.elasticsearch.entitlement.bridge.EntitlementChecker} instance (inside its
 * {@link org.elasticsearch.entitlement.bridge.EntitlementCheckerHandle} holder) and forwards the entitlement checks to the main library,
 * which is loaded normally.
 * {@link org.elasticsearch.entitlement.bridge.EntitlementChecker} holds all the entitlements check definitions, one for each instrumented
 * method.
 * </p>
 * <p>
 * <strong>NOTE:</strong> a subtle point that is worth noting: the {@link org.elasticsearch.entitlement.bridge.EntitlementChecker}
 * interface needs to be accessible from instrumented methods, hence the need to define it here in this project. But the reference held
 * by {@link org.elasticsearch.entitlement.bridge.EntitlementCheckerHandle} is an instance of a concrete class that need not be accessible.
 * Once things get underway, the instrumented code is directly calling a method of {@code ElasticsearchEntitlementChecker}, a class that
 * it could not resolve, by referencing that method through the {@link org.elasticsearch.entitlement.bridge.EntitlementChecker} interface.
 * </p>
 * <p>
 * In order to work across multiple Java versions, this project uses multi-release jars via the {@code mrjar} plugin, which makes it is
 * possible to specify classes for specific Java versions in specific {@code src} folders (e.g. {@code main23} for classes available to
 * Java 23+).
 * We use the mrjar plugin in a particular way, merging all the classes into the  same jar. Therefore, we want to prefix the file name
 * with the version, e.g. {@code Java23EntitlementCheckerHandle} and {@code Java23EntitlementChecker}.
 * </p>
 * <p>
 * At runtime, we identify and instantiate the correct class using the runtime Java version to prepend the correct prefix to the class
 * name  (see {@code EntitlementInitialization#getVersionSpecificCheckerClass}).
 * </p>
 * <p>
 * These different classes are needed to hold entitlements check definitions that are specific to a Java version.
 * As an example, consider the Linker API.
 * </p>
 * <p>
 * <strong>Note:</strong> the current version of Elasticsearch supports Java 21+; this is only an example (taken from the 8.x branch) that
 * illustrates a complex scenario.
 * </p>
 * <p>
 * The API went through multiple previews, and therefore changes between Java 19, 20 and 21; in order to support this correctly on these
 * versions, we should introduce 2 utility interfaces, "preview" and "stable".
 * For example, for the Java 20 specific signatures and functions, we would create {@code Java20StableEntitlementChecker} and
 * {@code Java20PreviewEntitlementChecker}.
 * </p>
 * <p>
 * The linker API in Java 20 introduces the final form for {@code downcallHandle}, which has different argument types from the one in
 * Java 19. To instrument and check it, we would add a
 * {@code check$jdk_internal_foreign_abi_AbstractLinker$downcallHandle(FunctionDescriptor, Linker.Option...)} method for it to the
 * {@code Java20StableEntitlementChecker} interface, which extends {@link org.elasticsearch.entitlement.bridge.EntitlementChecker}.
 * This interface would then be used by both the Java 20 specific interface ({@code Java20EntitlementChecker}) and any interface for newer
 * Java versions (e.g. {@code Java21EntitlementChecker}, which extends {@code Java20StableEntitlementChecker}): this way when we run on
 * either Java 20, Java 21, or following versions, we always instrument {@code downcallHandle} with the Java 20+ signature defined in
 * {@code Java20StableEntitlementChecker}.
 * Java 20 also introduces the {@code upcallStub} function; this function is not in its final form, as it has different parameters in the
 * following (21+) previews and in the final API.
 * In this case, we would add a {@code jdk_internal_foreign_abi_AbstractLinker$upcallStub(MethodHandle, FunctionDescriptor, SegmentScope)}
 * function to the {@code Java20PreviewEntitlementChecker} interface. {@code Java20EntitlementChecker} would inherit from this interface
 * too, but {@code Java21EntitlementChecker} and following would not. This way when we run on Java 20 we would instrument {@code upcallStub}
 * with the Java 20 signature {@code (FunctionDescriptor, Linker.Option...)}, but we would not when we run on following (Java 21+) versions.
 * Those will have the newer (final) {@code upcallStub} definition introduced in {@code Java21EntitlementChecker}.
 * </p>
 */
package org.elasticsearch.entitlement.bridge;
