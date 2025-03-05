### Entitlement library

This module implements mechanisms to grant and check permissions under the _entitlements_ system.

The entitlements system provides an alternative to the legacy `SecurityManager` system, which is deprecated for removal.
The `entitlement-agent` instruments sensitive class library methods with calls to this module, in order to enforce the controls.

This feature is currently under development, and it is completely disabled by default (the agent is not loaded). To enable it, run Elasticsearch with
```shell
./gradlew run --entitlements
```
