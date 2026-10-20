If the `FROM` combines that source with other sources, the requested metadata columns might appear with `null` values for rows from that source, but this behavior is not consistent.

To retrieve metadata values reliably, request the fields inside the source and retain them in its output.
