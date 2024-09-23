### Entitlement runtime

This module implements mechanisms to grant and check permissions under the _entitlements_ system.

The entitlements system provides an alternative to the legacy `SecurityManager` system, which is deprecated for removal.
The `entitlement-agent` tool instruments sensitive class library methods with calls to this module, in order to enforce the controls.

This module is responsible for:
- Defining which class library methods are sensitive
- Defining what permissions should be checked for each sensitive method
- Implementing the permission checks
- Offering a "grant" API to grant permissions

It is not responsible for anything to do with bytecode instrumentation; that responsibility lies with `entitlement-agent`.
