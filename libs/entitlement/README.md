# Entitlements

This module implements mechanisms to grant and check permissions under the _Entitlements_ system.

The entitlements system provides an alternative to the legacy Java Security Manager;
Elasticsearch makes heavy use of the Java Security Manager to minimize the risk of security vulnerabilities impacting Elasticsearch. The Java Security Manager has been [deprecated for removal since Java 17](https://openjdk.org/jeps/411) (Sept 2021) and is getting [removed in JDK 24](https://openjdk.org/jeps/486) (March 2025). The removal of the Security Manager would leave users more vulnerable to future security vulnerabilities like Log4Shell.

The goal of _Entitlements_  is not to write a full-fledged custom Security Manager, but to preserve the current level of protection against threats: Elasticsearch used the Java Security Manager to limit the ability to perform certain security-sensitive actions as part of its in-depth security mechanism (e.g. to limit the potential fallout from remote code execution (RCE) vulnerabilities). Entitlements will ensure that we maintain a comparable level of protection.

In practice, an entitlement allows code to call a well-defined set of corresponding JDK methods; without the entitlement calls to those JDK methods are denied and throw a `NotEntitledException`.

## Structure

The `agent` instruments sensitive JDK class library methods using a `InstrumentationService`. The current implementation of the instrumentation service uses ASM and is located under `asm-provider`.
The `agent` injects calls to the methods in the main module, via a `bridge` to overcome Class Loader and module layer issues. The main module implements Entitlement checking, mainly in the `PolicyManager` class; it also contains the implementation of the data objects used to define Entitlements (`Policy`, `Scope` and all classes implementing the `Entitlement` interface) as well as the logic for handling them (`PolicyParser`, `PolicyUtils`).

![Alt text](./entitlements-loading.svg)

## Policies

Policies are defined by the `Policy` class, which holds a list of `Scope`s.

The Entitlement model is _scope_-based: the subset of code to which we grant the ability to perform a security-sensitive action is called a _scope_ and implemented by the `Scope` class.
Currently, scope granularity is at the Java module level; in other words, an _entitlement scope_ corresponds to a Java module.
An _entitlement_ granted to a scope allows its code to perform the security-sensitive action associated with that entitlement. For example, the ability to read a file from the filesystem is limited to scopes that have the `files` entitlement for that particular file.

### How to add a server/module/plugin policy

A policy can be defined directly in code, or via a YAML policy file which is then parsed by `PolicyParser`.

Entitlements are divided into 3 categories:
- externally available (can be uses in a YAML file)
  - available to plugins
  - available only to Elasticsearch internal modules
- not externally available (can be used only to specify entitlements for the server layer)

How to add an Entitlements plugin policy is described in the official Elasticsearch docs on how to [create a classic plugin](https://www.elastic.co/guide/en/elasticsearch/plugins/current/creating-classic-plugins.html). The list of entitlements  available to plugins is also described there.

For Elasticsearch modules, the process is the same. In addition to the entitlements available for plugins, internal Elasticsearch modules can specify the additional entitlements:

#### `create_class_loader`
Allows code to set create a Java ClassLoader.

#### `write_all_system_properties`
This entitlement is similar to `write_system_properties`, but it's not necessary to specify the property names that code in the scope can write: all properties can be written by code with this entitlement.

#### `inbound_network`
This entitlement is currently available to plugins too; however, we plan to make it internally available only as soon as we can. It will remain available to internal Elasticsearch modules.

Entitlements for modules in the server layer are grouped in a "server policy"; this policy is built by Java code in `EntitlementsInitialization`. As such, it can use entitlements that are not externally available, namely `ReadStoreAttributesEntitlement` and `ExitVMEntitlement`.

Finally, there are some actions that are always denied; these actions do not have an associated entitlement, they are blocked with no option to make them allowed via a policy. Examples are: spawning a new process, manipulating files via file descriptors, starting a HTTP server, changing the locale, timezone, in/out/err streams, the default exception handler, etc.

## Tips

### What to do when you have a NotEntitledException

You can realize the code you are developing bumps into a `NotEntitledException`; that means your code/code you are referencing from a 3rd party library is attempting to perform a sensitive action and it does not have an entitlement for that, so we are blocking it.

A `NotEntitledException` could be handled by your code/by your library (via a try-catch, usually of `SecurityException`); in that case, you will still see a WARN log for the "Not entitled" call.

To distinguish these two cases, check the stack trace of the `NotEntitledException` and look for a frame that catches the exception.

If you find such a frame, then this `NotEntitledException` could be benign: the code knows how to handle a `SecurityException` and continue with an alternative strategy. In this case, the remedy is probably to suppress the warning.

If you do not find such a frame, then the remedy is to grant the entitlement.

#### Suppress a benign warning

For a benign `NotEntitledException` that is caught, we probably want to ignore the warning.
Double-check with Core/Infra before embarking on this, because it’s a nontrivial amount of work, involving multiple PRs in multiple repos, and you want to make sure this is the way to go before spending time on it.

Suppressing the warning involves adding a setting to the `log4j2.properties` files; you can follow [this PR](https://github.com/elastic/elasticsearch/pull/124883) as an example. Use a consistent naming convention, e.g. `logger.entitlements_<plugin_name>.name`. Avoid using extra dots, use `_` instead.
Each component has its own `log4j2.properties` file.
- Plugins: the file is placed in the plugin’s `config/<plugin-name>` directory. Ensure that its `build.gradle` file contains the logic to bundle the file in the plugin config:
```groovy
esplugin.bundleSpec.from('config/<plugin-name>') {
  into 'config'
}
```
- Modules + X-pack: place the file in `src/main/config`. The build process will take care of bundling the file.


#### Patching a policy via system properties

In an emergency, policies for Elasticsearch modules and plugins, and for the server layer modules, can be patched via a system property.
The system property is in the form `es.entitlements.policy.<plugin name>` (`es.entitlements.policy.server` for the server layer policy), and accepts a versioned policy:
```yaml
versions:
  - version1
  - versionN
policy:
  <a standard entitlement policy>
```
For example:
```yaml
versions:
  - 9.1.0
policy:
  ALL-UNNAMED:
    - set_https_connection_properties
    - outbound_network
    - files:
        - relative_path: ".config/gcloud"
          relative_to: home
          mode: read
```

The versioned policy needs to be base64 encoded. For example, to pass the above policy to a test cluster via gradle run:
```shell
./gradlew run --debug-jvm -Dtests.jvm.argline="-Des.entitlements.policy.repository-gcs=dmVyc2lvbnM6CiAgLSA5LjEuMApwb2xpY3k6CiAgQUxMLVVOTkFNRUQ6CiAgICAtIHNldF9odHRwc19jb25uZWN0aW9uX3Byb3BlcnRpZXMKICAgIC0gb3V0Ym91bmRfbmV0d29yawogICAgLSBmaWxlczoKICAgICAgLSByZWxhdGl2ZV9wYXRoOiAiLmNvbmZpZy9nY2xvdWQiCiAgICAgICAgcmVsYXRpdmVfdG86IGhvbWUKICAgICAgICBtb2RlOiByZWFkCg=="
```
The versions listed in the policy are string-matched against the Elasticsearch version as returned by `Build.version().current()`. It is possible to specify any number of versions.

The patch policy will be merged into the existing policy; in other words, entitlements specified in the patch policy will be **added** to the existing policy.

For example, if you add an entitlement to an existing scope:
```yaml
versions:
  - 9.1.0
policy:
  java.desktop:
    - manage_threads
```
with base64
```
dmVyc2lvbnM6CiAgLSA5LjEuMApwb2xpY3k6CiAgamF2YS5kZXNrdG9wOgogICAgLSBtYW5hZ2VfdGhyZWFkcw==
```
That policy is parsed and used to patch the existing entitlements to `java.desktop`, so at the end that module will have both the `load_native_libraries` (from the server layer embedded policy) and the  `manage_threads` entitlements (from the patch).

It is also possible to modify a current entitlement; for `files` and `write_system_properties`, this means that the 2 entitlements (from the patch and from the embedded policy) will be **merged**, taking fields from both of them, so you can grant access to additional files, upgrade access to `read_write`, or add a system property.
You can also add an entitlement to a new scope. It is not possible to remove an entitlement or a scope, or remove fields from an entitlement (e.g. remove access to a path or downgrade access to read-only).

If the policy is parsed and applied correctly, a INFO log will be displayed:
```
[INFO ][o.e.e.r.p.PolicyUtils	] [runTask-0] Using policy patch for layer [server]
```
If you try to add an invalid policy (syntax error, wrong scope, etc.) the patch will be discarded and Elasticsearch will run with the embedded policy. Same if the version does not match. In that case, you’ll see a WARN log:
```
[WARN ][o.e.e.r.p.PolicyUtils	] [runTask-0] Found a policy patch with invalid content. The patch will not be applied. Layer [server]
java.lang.IllegalStateException: Invalid module name in policy: layer [server] does not have module [java.xml]; available modules [...]; policy path [<patch>]
```

IMPORTANT: this patching mechanism is intended to be used **only** for emergencies; once a missing entitlement is identified, the fix needs to be applied to the codebase, by raising a PR or submitting a bug via Github so that the embedded policies can be fixed.

### How to migrate a from a Java Security Policy to an entitlement policy

Translating Java Security Permissions to Entitlements is usually not too difficult;
- many permissions are not used anymore. The Entitlement system is targeting sensitive actions we identified as crucial to our code; any other permission is not checked anymore. Also, we do not have  any entitlement related to reflection or access checks: Elasticsearch runs modularized, and we leverage and trust the Java module mechanism to enforce access and visibility.
Examples of permissions that do not have an Entitlement equivalent:
  - `java.net.NetPermission "getProxySelector"`, `java.util.PropertyPermission "<name>", "read"` or `java.lang.RuntimePermission "getClassLoader"`: we do not care about anything that "reads" or "gets" something. We care about writing or setting (except network and files);
  - `javax.security.auth.*Permission` or `java.security.SecurityPermission`: currently, we do not have any equivalent to authn/authz permissions. This could change in a future release.
  - `java.lang.reflect.ReflectPermission "suppressAccessChecks";` or  `java.lang.RuntimePermission "accessDeclaredMembers"`: we rely on Java module encapsulation to protect sensitive classes and methods.
  - `java.net.SocketPermission "*", "resolve"`
- some permissions have a 1-1 translation. Examples:
  - `java.net.SocketPermission "*", "connect"` translates to `outgoing_network`
  - `java.net.SocketPermission "*", "accept"` or `listen` translates to `incoming_network`
  - `java.lang.RuntimePermission "createClassLoader"` translates to `create_class_loader`
  - `java.io.FilePermission` translates to `files`
  - `java.util.PropertyPermission "<name>", "write"` translates to `write_system_properties` (`write_all_system_properties` in case `<name>` is `"*"`)
  - `java.lang.RuntimePermission "setContextClassLoader"` translates to `manage_threads`
  - `java.lang.RuntimePermission "loadLibrary*"` translates to `load_native_libraries`
- some permissions need more investigation:
  - `java.lang.RuntimePermission "setFactory"`: most of the methods that used to be guarded by this permission are always denied; some are always granted. The only equivalent in the entitlement system is `set_https_connection_properties`, for methods like `HttpsURLConnection.setSSLSocketFactory` that can be used to change a HTTPS connection properties after the connection object has been created.

Note however that the key difference between Security Manager and Entitlements in the policy check model means that translating a Security Manager policy to an Entitlement policy may not be a 1-1 translation from Permissions to Entitlements: Security Manager used to do a full-stack check, interrupted by `doPrivileged` blocks; Entitlements does a "first untrusted frame" check. This means that some Permissions that needed to be granted with Security Manager may not need the equivalent entitlement; conversely, code that used `doPrivileged` under the Security Manager model might have not needed a Permission, but might need an Entitlement now to run correctly.

Finally, a word on scopes: the Security Manager model used either general grants, or granted some permission to a specific codebase, e.g. `grant codeBase "${codebase.netty-transport}"`.
In Entitlements, there is no option for a general grant: you must identify to which module a particular entitlement needs to be granted (except for non-modular plugins, for which everything falls under `ALL-UNNAMED`). If the Security Manager policy specified a codebase, it's usually easy to find the correct module, otherwise it might be tricky and require deeper investigation.
