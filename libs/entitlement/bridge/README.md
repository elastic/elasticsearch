### Entitlement Bridge

This is the code called directly from instrumented methods.
It's a minimal shim that is patched into the `java.base` module
so that it is callable from the class library methods instrumented by the agent.
Its job is to forward the entitlement checks to the main library,
which is loaded normally.

It is not responsible for injecting the bytecode instrumentation (that's the agent)
nor for implementing the permission checks (that's the main library).

