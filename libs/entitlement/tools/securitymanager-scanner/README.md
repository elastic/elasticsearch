This tool scans the JDK on which it is running, looking for any location where `SecurityManager` is currently used, thus giving us a list of "entry points" inside the JDK where security checks are currently happening.

More in detail, the tool scans for calls to any `SecurityManager` method starting with `check` (e.g. `checkWrite`). The tool treats the generic `checkPermission` method a little bit differently: `checkPermission` accepts a generic `Permission` object, it tries to read the permission type and permission name to give more information about it, trying to match two patterns that are used frequently inside the JDK:

Pattern 1: private static permission field

```java
private static final RuntimePermission INET_ADDRESS_RESOLVER_PERMISSION =
new RuntimePermission("inetAddressResolverProvider");
...
sm.checkPermission(INET_ADDRESS_RESOLVER_PERMISSION);
```
Pattern 2: direct object creation

```java
sm.checkPermission(new LinkPermission("symbolic"));
```

The tool will recognize this pattern, and report the permission type and name alongside the `checkPermission` entry point (type `RuntimePermission` and name `inetAddressResolverProvider` in the first case, type `LinkPermission` and name `symbolic` in the second).

This allows to give more information (either a specific type like `LinkPermission`, or a specific name like `inetAddressResolverProvider`) to generic `checkPermission` to help in deciding how to classify the permission check. The 2 patterns work quite well and cover roughly 90% of the cases.

In order to run the tool, use:
```shell
./gradlew :libs:entitlement:tools:securitymanager-scanner:run
```
The output of the tool is a CSV file, with one line for each entry-point, columns separated by `TAB`

The columns are:
1. Module name
2. File name (from source root)
3. Line number
4. Fully qualified class name (ASM style, with `/` separators)
5. Method name
6. Method descriptor (ASM signature)
6. Visibility (PUBLIC/PUBLIC-METHOD/PRIVATE)
7. Check detail 1 (method name, or in case of checkPermission, permission name. Might be `MISSING`)
8. Check detail 2 (in case of checkPermission, the argument type (`Permission` subtype). Might be `MISSING`)

Examples:
```
java.base       sun/nio/ch/DatagramChannelImpl.java     1360    sun/nio/ch/DatagramChannelImpl  connect (Ljava/net/SocketAddress;Z)Ljava/nio/channels/DatagramChannel;  PRIVATE checkConnect
```
or
```
java.base       java/net/ResponseCache.java     118     java/net/ResponseCache  setDefault      (Ljava/net/ResponseCache;)V     PUBLIC  setResponseCache        java/net/NetPermission
```
