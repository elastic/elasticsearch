### Entitlement Agent

This is a java agent that instruments sensitive class library methods with calls into the `entitlement-bridge` module to check for permissions granted under the _entitlements_ system.

The entitlements system provides an alternative to the legacy `SecurityManager` system, which is deprecated for removal.
With this agent, the Elasticsearch server can retain some control over which class library methods can be invoked by which callers.

This module is responsible for inserting the appropriate bytecode to achieve enforcement of the rules governed by the `entitlement-runtime` module.

It is not responsible for permission granting or checking logic. That responsibility lies with `entitlement-runtime`.
