/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

/// Implements the Elasticsearch Entitlement System. The Entitlement system has some basic ingredients:
///
///   -
///     **Load policies** for the various layers
///
///   -
///     **Instrumentation** of JDK methods that perform sensitive actions to inject calls to a checker
///
///   -
///     **Caller identification**: identify the class that is responsible for the sensitive action
///
///   -
///     **Map to a policy**: find the set of entitlements granted to the caller. We do that by identifying the "layer" (server,
///     agent, ES plugin/module) and the associated policy, and the caller class module to identify which scope within the policy.
///
///   -
///     **Check**: does the set of entitlements grant the caller to perform the sensitive action? In other words, is the caller
///     entitled to call that JDK method?
///
///
/// ## Load policies
///
/// Policies for ES plugins and modules are stored in a YAML file bundled with the module/plugin
/// (@see <a href="https://github.com/elastic/elasticsearch/blob/main/libs/entitlement/README.md">README.md</a> for details). The files are
/// extracted from the bundles and parsed during Elasticsearch initialization (`Elasticsearch#initPhase2`); at the same time, we parse
/// both plugin and server policy patches from the command line. Patches to plugin policies are applied immediately, the server policy patch
/// is passed down to [org.elasticsearch.entitlement.bootstrap.EntitlementBootstrap].
///
///
/// The server and agent (APM) policies are created in EntitlementInitialization, just before the creation of
/// [org.elasticsearch.entitlement.runtime.policy.PolicyManager]. The server policy patch (if any) is read from the
/// [org.elasticsearch.entitlement.bootstrap.EntitlementBootstrap] arguments and applied here.
///
///
/// [org.elasticsearch.entitlement.initialization.EntitlementInitialization] creates
/// [org.elasticsearch.entitlement.runtime.policy.PolicyManager] passing down the policies it just created (server and agent) and the
/// plugin policies it read from [org.elasticsearch.entitlement.bootstrap.EntitlementBootstrap].
///
/// ## Instrumentation
///
/// Instrumentation happens dynamically via a Java Agent (`EntitlementAgent`, see the `agent` subproject).
///
///
/// The Agent is loaded dynamically by [org.elasticsearch.entitlement.bootstrap.EntitlementBootstrap]. We load the agent dynamically
/// because we don't want to define additional permissions that server would need; we perform several sensitive actions once during
/// Elasticsearch initialization. By initializing entitlements after we have performed those actions, we are able to never allow certain
/// actions, like process execution. An additional benefit is that  we are able to collect all the information needed before creating the
/// entitlement objects, so they can be immutable.
///
///
/// `EntitlementAgent` creates [org.elasticsearch.entitlement.initialization.EntitlementInitialization] and calls
/// [org.elasticsearch.entitlement.initialization.EntitlementInitialization#initialize(java.lang.instrument.Instrumentation)]
/// on it, both by using reflection. Agents are loaded into the unnamed module, which makes module exports awkward. To work around this,
/// we keep minimal code in the agent itself, and instead use reflection to call into this library.
/// [org.elasticsearch.entitlement.bootstrap.EntitlementBootstrap] uses [java.lang.Module#addExports] to export
/// [org.elasticsearch.entitlement.initialization.EntitlementInitialization] to the agent and make it available.
///
///
/// [org.elasticsearch.entitlement.initialization.EntitlementInitialization] loads an
/// [org.elasticsearch.entitlement.instrumentation.InstrumentationService] instance.
/// [org.elasticsearch.entitlement.instrumentation.InstrumentationService] is an interface that encapsulates all bytecode manipulation
/// operations. We use SPI to load an implementation for it; currently, the implementation uses ASM, and it is located in the
/// `asm-provider` subproject.
///
/// ### How we identify the methods to instrument
///
/// [org.elasticsearch.entitlement.initialization.EntitlementInitialization] builds the set of methods to instrument using a
/// declarative configuration system. Instrumentation rules are defined in implementation classes of
/// [org.elasticsearch.entitlement.config.InstrumentationConfig]. The main provider
/// [org.elasticsearch.entitlement.config.MainInstrumentationProvider] aggregates instrumentation configurations organized by
/// functional area (e.g., [org.elasticsearch.entitlement.config.FileInstrumentation],
/// [org.elasticsearch.entitlement.config.NetworkInstrumentation], [org.elasticsearch.entitlement.config.SystemInstrumentation]).
///
///
/// These configurations use a fluent builder API provided by [org.elasticsearch.entitlement.rules.EntitlementRulesBuilder]
/// to declaratively specify which methods to instrument. The builder provides:
///
///
///   - **Method selection**: Use [org.elasticsearch.entitlement.rules.ClassMethodBuilder] to select target methods via method
///     references, supporting both instance and static methods.
///
///   - **Check specification**: Define the policy check to perform using factory methods in [org.elasticsearch.entitlement.rules.Policies],
///     which create [org.elasticsearch.entitlement.rules.function.CheckMethod] instances.
///
///   - **Denial strategies**: Specify what happens when an entitlement check fails using
///     [org.elasticsearch.entitlement.rules.DeniedEntitlementStrategy], such as throwing
///     [org.elasticsearch.entitlement.bridge.NotEntitledException] or returning early with a specific value.
///
///
/// These rules are registered with [org.elasticsearch.entitlement.runtime.registry.InternalInstrumentationRegistry], which
/// stores [org.elasticsearch.entitlement.runtime.registry.InstrumentationInfo] objects containing the method signature,
/// check method, and denial strategy. The registry is accessible via
/// [org.elasticsearch.entitlement.bridge.InstrumentationRegistry], which needs to be accessible by both this project and the
/// code injected by the agent, and is therefore located in the `bridge` subproject.
///
/// ### How that works across different Java versions
///
/// The entitlement project uses multi-release jars via the `mrjar` plugin, which makes it possible to specify classes for
/// specific Java versions in version-specific `src` folders (e.g., `src/main22` for classes available to Java 22+).
///
///
/// Version-specific instrumentation rules are provided by overriding [org.elasticsearch.entitlement.config.MainInstrumentationProvider]
/// in these version-specific source sets. At runtime, the JVM automatically loads the appropriate version of
/// `MainInstrumentationProvider` based on the running Java version. These version-specific providers typically extend the base
/// provider by adding instrumentation configurations for APIs that were introduced or changed in that Java version.
///
///
/// For example, the Java 22+ version of `MainInstrumentationProvider` (located in `src/main22`) includes
/// [org.elasticsearch.entitlement.config.ClassFileInstrumentation] in addition to all the base instrumentation configurations.
/// This allows instrumenting `ClassFile` API methods that were introduced in Java 22 without affecting earlier Java versions.
///
///
/// This approach provides clean separation of version-specific instrumentation rules while maintaining a consistent declarative
/// configuration style across all Java versions. Each version-specific provider can:
///
///
///   - Add new instrumentation configurations for APIs introduced in that Java version
///   - Override base configurations if method signatures changed between versions
///   - Selectively include or exclude specific instrumentation rules
///
///
/// The multi-release jar mechanism ensures that only the appropriate version-specific code is loaded at runtime, preventing
/// `ClassNotFoundException` or `NoSuchMethodError` issues when running on different Java versions.
///
/// ## Prologue injection
///
/// Agents get access to the Java instrumentation API by receiving a [java.lang.instrument.Instrumentation] instance, which we pass
/// to [org.elasticsearch.entitlement.initialization.EntitlementInitialization#initialize] to setup code needed to transform classes
/// as they get loaded. See [org.elasticsearch.entitlement.initialization.EntitlementInitialization#initialize] for details.
///
///
/// Our implementation instrument classes by adding a prologue to the methods identified in
/// [org.elasticsearch.entitlement.initialization.EntitlementInitialization] (see previous section).
/// See [org.elasticsearch.entitlement.instrumentation.Instrumenter#instrumentClass] for details.
///
/// ## Caller identification
///
/// In order to verify if a method is entitled to perform an action, we need to identify the right policy to check; the first step here is
/// to identify the caller. This is done in the injected prologue, via a helper function
/// [org.elasticsearch.entitlement.bridge.Util#getCallerClass], which performs a limited stack walk.
///
/// ## Map to a policy
/// ### Identify the "layer"
///
/// The first step to find the set of entitlements granted to the caller class is to find the "layer" that hosts the class/module.
/// Each layer may have a policy attached to it (1-1* relationship).
///
///
/// This starts during Elasticsearch initialization (`initPhase2`), just after policies are parsed but before entitlements are
/// initialized via [org.elasticsearch.entitlement.bootstrap.EntitlementBootstrap], through a new class named `PluginsLoader`.
/// Before 8.18, `PluginsServices` (created as part of `Node` initialization in `initPhase3`) had 2 concerns:
/// create the "infrastructure" to load an ES plugin (or ES module), e.g. the module layer and class loader, and actually load the main
/// plugin class and create an instance of it for the plugin.
/// Now the first concern (create the module layer and class loader) has been refactored and moved to `PluginsLoader`, so it can
/// happen separately and earlier, in Phase 2, before entitlements are initialized.
///
///
/// The module layers and class loaders are used to map a class to a layer (via the `PluginsResolver` class): we use them to build a
/// Module -> Plugin name (String)  map. For modularized plugins we use the list of modules defined in the module layer; for the
/// non-modularized ones, we use the unnamed module which is unique to each plugin classloader.
///
///
/// This map is then passed down and stored by [org.elasticsearch.entitlement.runtime.policy.PolicyManager]. Alongside this map,
/// [org.elasticsearch.entitlement.runtime.policy.PolicyManager] builds a set of references to modules
/// that belong to what we call the "system layer", i.e. the layer containing what we consider system modules, and the set of modules
/// that we consider belonging to the "server layer".
/// [org.elasticsearch.entitlement.runtime.policy.PolicyManager] uses this info to identify the layer, and therefore the policy and
/// entitlements, for the caller class.
///
///
/// See [org.elasticsearch.entitlement.runtime.policy.PolicyManager] for details.
///
/// ## Checks
///
/// The injected prologue retrieves the [org.elasticsearch.entitlement.rules.function.CheckMethod] associated with the instrumented
/// method from [org.elasticsearch.entitlement.bridge.InstrumentationRegistry] and invokes it with the caller class. The check method
/// implementation calls the appropriate methods on [org.elasticsearch.entitlement.runtime.policy.PolicyChecker], forwarding the
/// caller class and method-specific arguments. These checker methods correspond to specific entitlement types (e.g.,
/// [org.elasticsearch.entitlement.runtime.policy.PolicyChecker#checkInboundNetworkAccess],
/// [org.elasticsearch.entitlement.runtime.policy.PolicyChecker#checkFileRead]).
///
///
/// If the check fails, the configured [org.elasticsearch.entitlement.rules.DeniedEntitlementStrategy] determines the behavior:
/// most commonly throwing [org.elasticsearch.entitlement.bridge.NotEntitledException], but potentially returning early with a
/// default value or the original method argument.
///
///
/// Most of the entitlements are "flag" entitlements: when present, it grants the caller the right to perform an action (or a set of
/// actions); when it's not present, the actions associated with it are denied. Checking is simply a fact checking if the entitlement type
/// is present or not.
///
/// There are two entitlements that are not simple flags:
///
///   - system properties, where we further get the instance of the entitlement for the
///     [org.elasticsearch.entitlement.runtime.policy.entitlements.WriteSystemPropertiesEntitlement] class, and we check if it contains
///     that specific property name, and
///
///   -
///     file access, which is treated separately for convenience and performance reasons.
///     See [org.elasticsearch.entitlement.runtime.policy.FileAccessTree] for details.
///
///
///
/// A final special cases that short circuit the checks (resulting in a "trivially allowed" case) is when the caller is null is the special
/// `NO_CLASS` tag class - this happens if there are no frames in the call stack, e.g. when a call originated directly from native
/// code (the JVM itself, a callback stub, a debugger, ...).
///
package org.elasticsearch.entitlement;
