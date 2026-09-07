/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasources.spi;

import org.elasticsearch.xpack.esql.core.QlException;

/**
 * Base type for failures raised while reading from an external data source — an object store, a
 * file in some columnar/row format, a remote HTTP endpoint, etc. Grouping every external-source
 * failure under one type lets callers catch them as a family ({@code catch (ExternalException)})
 * and lets external-read operators surface them with the correct HTTP status.
 * <p>
 * The distinction between server- and client-class failures is carried by the concrete subtype, so
 * the right status falls out of the exception itself rather than from a downstream {@code instanceof}
 * ladder:
 * <ul>
 *     <li>{@link ExternalClientException} &rarr; 400, the request pointed us at something we cannot
 *     read or decode (bad/unsupported input, missing object).</li>
 *     <li>{@link ExternalServerException} &rarr; 500, a bug or broken invariant in our own reading
 *     code.</li>
 *     <li>{@link ExternalUnavailableException} &rarr; 503, a retryable back-pressure /
 *     temporarily-unavailable condition (a remote-store transport failure, or a node-local admission
 *     condition such as permit exhaustion).</li>
 * </ul>
 * Subtypes extend {@link QlException} (rather than {@code QlClientException}/{@code QlServerException})
 * so they can share this single umbrella while each pinning its own status.
 * <p>
 * <b>Transport behaviour.</b> Like every other ES|QL exception, these are not registered in
 * {@code ElasticsearchException}'s serialization registry, so crossing a node boundary turns them
 * into a {@code NotSerializableExceptionWrapper}. That wrapper preserves both the public exception
 * name (for example, {@code external_client_exception}) and {@link #status()} (it captures
 * {@code ExceptionsHelper.status(this)} on the sending node and replays it on the receiver), so the
 * name and 400/500/503 distinction survive the data-node &rarr; coordinator hop and the REST layer
 * still maps them correctly. What does <em>not</em> survive is the concrete Java type: a remote
 * receiver cannot {@code instanceof}-check these. That is fine because classification happens
 * co-located with the throw — {@code ExternalFailures.classify} runs inside
 * {@code AsyncExternalSourceOperator} for eager reads and {@code ExternalFieldExtractOperator} for
 * deferred reads, on the node that reads the external source and before any serialization — so no
 * remote consumer ever needs the concrete type.
 */
public abstract class ExternalException extends QlException {

    protected ExternalException(String message, Throwable cause) {
        super(message, cause);
    }

    protected ExternalException(Throwable cause, String message, Object... args) {
        super(cause, message, args);
    }

    protected ExternalException(String message, Object... args) {
        super(message, args);
    }
}
