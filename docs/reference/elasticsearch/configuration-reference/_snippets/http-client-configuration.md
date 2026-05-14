Don't use response timeouts in your HTTP client. Instead, use TCP keepalives to detect network outages. {{es}} always responds to every request, but some requests can take several minutes to complete. Many HTTP clients default to short timeouts suited for browser interactions, which can cause them to report a failure before {{es}} has finished processing.

Response timeouts can cause significant issues:

* **Retries increase load.** When you retry a timed-out request, {{es}} places it at the back of the same queue. The request takes even longer to complete, and the retry adds extra load on the cluster.
* **Non-idempotent requests can't be retried safely.** If a request can't be retried, a timeout leaves you with no recourse. Waiting for a response gives the operation a chance to succeed.

TCP keepalives detect network outages without imposing a time limit on request processing.
