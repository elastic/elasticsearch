# Entitlements

This module implements mechanisms to grant and check permissions under the _Entitlements_ system.

The entitlements system provides an alternative to the legacy Java Security Manager;
Elasticsearch (ES) has previously made heavy use of the Java Security Manager to minimize the risk of security vulnerabilities impact. The Java Security Manager has been [deprecated for removal since Java 17](https://openjdk.org/jeps/411) (Sept 2021) and has been [removed in JDK 24](https://openjdk.org/jeps/486) (March 2025). Without an alternative, the removal of the Java Security Manager would have left Elasticsearch users more susceptible to future security vulnerabilities.

The goal of _entitlements_ is to protect certain sensitive operations on resources, and the JVM itself, from unexpected and unwanted access, e.g. to limit the scope of potential remote code execution (RCE) vulnerabilities.

In practice, an entitlement allows code to call a well-defined set of corresponding JDK methods; without the entitlement code calling into those JDK methods is blocked and gets a `NotEntitledException`.

## Structure

All the code implementing Entitlements can be found under this directory. The `agent` module bootstraps the entitlement lib, and uses it to instruments sensitive JDK class library methods using a `InstrumentationService`. The current implementation of the instrumentation service uses ASM and is located under `asm-provider`.

`InstrumentationService` transform JDK methods to start with a call to check entitlements. The entitlement checker is defined in the `bridge`, which is patched into `java.base` at runtime because it must exist in the platform classloader.

The entitlement checker is implemented in the entitlement lib, which the `bridge` grabs reflectively. `PolicyManager` is where most checks are actually done. The entitlement lib also contains the implementation of the data objects used to define Entitlements (`Policy`, `Scope` and all classes implementing the `Entitlement` interface) as well as the logic for handling them (`PolicyParser`, `PolicyUtils`).

![Alt text](./entitlements-loading.svg)

## Policies

A `Policy` is associated with a single `component` (i.e. Elasticsearch module/plugin or server) and represents the entitlements allowed for a particular `Scope` (i.e. Java module).

Entitlements are divided into 3 categories:
- available everywhere (Elasticsearch module/plugin or server)
- available only to Elasticsearch modules
- not externally available: can be used only to specify entitlements for modules in the server layer.

In order to help developers adding the correct entitlements to a policy, the name of the component, the scope name (Java module) and the name of the missing entitlement are specified in the `NotEntitledException` message:
```
NotEntitledException: component [(server)], module [org.apache.lucene.misc], class [class org.apache.lucene.misc.store.DirectIODirectory], entitlement [read_store_attributes]
```

### How to add an Elasticsearch module/plugin policy

A policy is defined in an `entitlements-policy.yaml` file within an Elasticsearch module/plugin under `src/main/plugin-metadata`. Policy files contain lists of entitlements that should be allowed, grouped by Java module name, which acts as the policy scope. For example, the `transport-netty4` Elasticsearch module's policy file contains an entitlement to accept `inbound_network` connections, limited to the `io.netty.transport` and `io.netty.common` Java modules.

Elasticsearch modules/plugins that are not yet modularized (i.e. do not have `module-info.java`) will need to use single `ALL-UNNAMED` scope. For example, the `reindex` Elasticsearch module's policy file contains a single `ALL-UNNAMED` scope, with an entitlement to perform `outbound_network`; all code in `reindex` will be able to connect to the network. It is not possible to use the `ALL-UNNAMED` scope for modularized modules/plugins.

How to add an Entitlements plugin policy is described in the official Elasticsearch docs on how to [create a classic plugin](https://www.elastic.co/guide/en/elasticsearch/plugins/current/creating-classic-plugins.html). The list of entitlements available to plugins is also described there.

For Elasticsearch modules, the process is the same. In addition to the entitlements available for plugins, Elasticsearch modules can specify the additional entitlements:

#### `create_class_loader`
Allows code to construct a Java ClassLoader.

#### `write_all_system_properties`
This entitlement is similar to `write_system_properties`, but it's not necessary to specify the property names that code in the scope can write: all properties can be written by code with this entitlement.

#### `inbound_network`
This entitlement is currently available to plugins too; however, we plan to make it internally available only as soon as we can. It will remain available to Elasticsearch modules.

### How to add a server layer entitlement

Entitlements for modules in the server layer are grouped in a "server policy"; this policy is builtin into Elasticsearch, expressed in Java code in `EntitlementInitialization` (see `EntitlementInitialization#createPolicyManager`). As such, it can use entitlements that are not externally available, namely `ReadStoreAttributesEntitlement` and `ExitVMEntitlement`.

In order to add an entitlement, first look if the scope is already present in the server policy. If it's not present, add one. If it is, add an instance of the correct entitlement class to the list of entitlements for that scope.
There is a direct mapping between the entitlement name and the Entitlement class: the name is written in snake case (e.g. `example_name`), the corresponding class has the same name but in Pascal Case with the addition of a `Entitlement` suffix (e.g. `ExampleNameEntitlement`).

For example, to fix the `NotEntitledException` from the example above:
```java
new Scope(
   "org.apache.lucene.misc",
    List.of(
       new FilesEntitlement(List.of(FileData.ofRelativePath(Path.of(""), DATA, READ_WRITE))),
       new ReadStoreAttributesEntitlement() // <- add this new entitlement
    )
)
```
In any case, before adding a `server` entitlement or make any change to the server layer policy, please consult with the Core/Infra team.

### Always denied

Finally, there are some actions that are always denied; these actions do not have an associated entitlement, they are blocked with no option to allow them via a policy. Examples are: spawning a new process, manipulating files via file descriptors, starting a HTTP server, changing the locale, timezone, in/out/err streams, the default exception handler, etc.

## Tips

### What to do when you have a NotEntitledException

You can realize the code you are developing bumps into a `NotEntitledException`; that means your code (or code you are referencing from a 3rd party library) is attempting to perform a sensitive action, and it does not have an entitlement for that, so we are blocking it.

A `NotEntitledException` could be handled by your code/by your library (via a try-catch, usually of `SecurityException`); in that case, you will still see a WARN log for the "Not entitled" call.

To distinguish these two cases, check the stack trace of the `NotEntitledException` and look for a frame that catches the exception.

If you find such a frame, then this `NotEntitledException` could be benign: the code knows how to handle a `SecurityException` and continue with an alternative strategy. In this case, the remedy is probably to suppress the warning.

If you do not find such a frame, then the remedy is to grant the entitlement.
If the entitlement should or could not be granted (e.g. because the offending code is trying to start a process), then you need to modify your code so it does not perform the sensitive (and forbidden) operation anymore.

#### Suppress a benign warning

For a benign `NotEntitledException` that is caught, we probably want to suppress (ignore) the warning.

Suppressing the warning involves adding two lines to the `log4j2.properties` files: one specifying the logger name,
and the other specifying the level of `ERROR`, thereby silencing the warning.
Each component has its own `log4j2.properties` file, but they are bundled together by the build process, so make sure to use a globally unique name for each setting you add.
The naming convention is `logger.entitlements_<plugin_name>.name`.
If a plugin needs more than one override, additional suffixes can be added to `<plugin_name>` to distinguish them.
The `<plugin_name>` portion of the name must contain no dots or other punctuation besides underscores.

You can follow [this PR](https://github.com/elastic/elasticsearch/pull/124883) as an example.

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

The versioned policy needs to be base64 encoded, e.g. by placing the policy in a file like `plugin-patch.yaml` and the `base64` command line tool which is included in many OSes:
```shell
base64 -i plugin-patch.yaml
```
The base64 string will then need to be passed via the command line to ES.
For example, to pass the above policy to a test cluster via gradle run:
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

IMPORTANT: this patching mechanism is intended to be used **only** for emergencies; once a missing entitlement is identified, the fix needs to be applied to the codebase, by raising a PR or submitting a bug via Github so that the bundled policies can be fixed.

### How to migrate from a Java Security Manager Policy to an entitlement policy

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

Note however that there is a key difference in the policy check model between Security Manager and Entitlements. This means that translating a Security Manager policy to an Entitlement policy may not be a 1-1 mapping from Permissions to Entitlements.  Security Manager used to do a full-stack check, truncated by `doPrivileged` blocks; Entitlements check the "first untrusted frame". This means that some Permissions that needed to be granted with Security Manager may not need the equivalent entitlement; conversely, code that used `doPrivileged` under the Security Manager model might have not needed a Permission, but might need an Entitlement now to run correctly.

Finally, a word on scopes: the Security Manager model used either general grants, or granted some permission to a specific codebase, e.g. `grant codeBase "${codebase.netty-transport}"`.
In Entitlements, there is no option for a general grant: you must identify to which module a particular entitlement needs to be granted (except for non-modular plugins, for which everything falls under `ALL-UNNAMED`). If the Security Manager policy specified a codebase, it's usually easy to find the correct module, otherwise it might be tricky and require deeper investigation.
