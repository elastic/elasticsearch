# Liveness

Resources must be released eventually.
Listeners must be completed eventually.
Write code so that its liveness properties are clear to the reader.
Rely on the tools, frameworks and infrastructure within Elasticsearch to ensure clear liveness guarantees wherever possible.
Avoid liveness conditions which require complex reasoning across many pages of code as they often harbor (present or future) bugs.
If complex reasoning is unavoidable, document the liveness argument within the code.

## Resources

A resource is a conceptual entity which requires some kind of active cleanup at the end of its life.
This includes:

* file handles (must be closed)
* pooled memory allocations (must be released back to the pool)
* circuit-breaker reservations (must be freed)
* each increment on a ref-counted object (must have a corresponding decrement)
* objects that take ownership of one or more other resources (must clean up all owned resources)

Plain Java heap allocations are not resources because the JVM GC automatically cleans them up.

If the lifecycle of a resource is unclear, or its ownership is transferred in a way that is hard to understand, then there is a risk of a liveness bug.

## Local reasoning

Not all liveness properties can be established with purely local reasoning, but the smaller the size of the liveness argument the better.

Code which calls an asynchronous method may usually assume that the called method will either eventually complete the provided listener, or it throws an exception.

Code which acquires a resource should make it clear to the reader how that resource is released.
This often means one of:
* closing the resource in a `finally` block,
* attaching the release of the resource to the completion of an existing listener using `ActionListener.releaseBefore` or `ActionListener.releaseAfter` or similar,
* converting the release of the resource to the completion of a new listener using `ActionListener.releasing` or similar, if there is no existing listener in scope,
* transferring the ownership of the resource into another resource whose eventual release is guaranteed.

### Node shutdown

For the sake of simplicity, ignore liveness issues that only occur when the node shuts down.

### Timeouts

A timeout, keep-alive, or reaper does not discharge a release that some other owner is supposed to perform. Judge the obligation with that timer removed.

A single-session search reader context is released by the search that opened it, when that search finishes, on success and on failure. `SearchService`'s keep-alive reaper frees a context only if that release never happens. A path that leaves the context to the reaper is a liveness bug.

A PIT or scroll context is released by the client that holds its id. Assume the client closes it. The keep-alive covers a client that never does. Returning the id, and releasing the context on the client's close, are the obligations. A context the client still holds is not a leak. A context left only for the reaper, because the id was dropped or the close was not honored, is a liveness bug. A transient thread-pool rejection on a scroll continuation keeps the context so the client can retry; that is the release path for the retry.

The same rule applies to any other timeout whose only effect is to finish a listener or free a resource that an earlier path failed to finish or free.

## Principles

Methods which do not work with `ActionListener` or equivalent, do not acquire a resource, and do not block a thread, have no liveness issues. Ignore blocking on disk I/O when considering liveness: disk I/O always eventually completes, and our focus here is on liveness and not performance.

Asynchronous methods which accept an `ActionListener` must on every path either complete the listener directly or pass the listener on to another asynchronous method.
Many such methods may throw an exception instead of completing the listener.
If the method also acquires resources, it must either ensure that those resources are released on exception or else justify why no exception is possible.
It is confusing to pass the listener elsewhere for asynchronous completion and then synchronously throw an exception: if this is deliberate, it deserves a comment.

Methods which create a `GroupedActionListener` must ensure that it is eventually completed the correct number of times, which must not be zero.

Methods which create a `RefCountingListener` must ensure that it is eventually closed, and that every acquired listener is eventually completed.

Methods which create a `RefCountingRunnable` must ensure that it is eventually closed, and that every acquired reference is eventually closed.

Subsystems which accumulate a collection (e.g. a queue) of listeners, or of resources, must ensure that the collection is eventually drained.

A common pattern is for an asynchronous method to partially modify the behavior of its listener using utilities such as `ActionListener#delegateFailure` and `ActionListener#delegateResponse`, passing
the modified listener on to other asynchronous code. The modifying callback must complete its listener argument on all paths. Use `ActionListener#delegateFailureAndWrap` if the callback might throw an
exception.

## Reasoning heuristics

Assume that `AbstractRunnable` tasks submitted to a threadpool eventually execute, or are eventually notified of failure or rejection. Submitting an `AbstractRunnable` to a threadpool never throws an exception.

Assume that tasks submitted for execution by the `MasterService` eventually complete.

Assume that actions invoked via the `TransportService` eventually complete. The lifecycle of requests submitted to the `TransportService` is described in the Javadoc for `TransportService#sendRequest`, and the lifecycle of the eventually-received response object is described in the Javadoc on `TransportResponseHandler#handleResponse`.

Assume that assertions are disabled, but that the condition in every assertion is satisfied.

Utilities which decorate an `ActionListener` with extra behavior (such as the static methods on `ActionListener` itself) split the reasoning path in two: the decorated listener must always eventually be completed, and completing the decorated listener must always complete the original listener.
