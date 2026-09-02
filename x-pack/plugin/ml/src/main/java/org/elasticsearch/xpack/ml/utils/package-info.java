/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

/**
 * Cross-cutting ML utilities shared across the ML plugin.
 *
 * <h2>Error Classification</h2>
 * <p>
 * {@link org.elasticsearch.xpack.ml.utils.MlRecoverableErrorClassifier MlRecoverableErrorClassifier}
 * is a stateless utility that classifies exceptions as <i>recoverable</i> (transient, safe to
 * retry) or <i>irrecoverable</i> (permanent, should fail fast). It uses an explicit allowlist
 * across four infrastructure layers, defaulting to irrecoverable for unknown exception types:
 * <p>
 * 1. <b>Data Plane</b> — {@code SearchPhaseExecutionException}: shard-level search failures that
 *    resolve when shards recover.
 * <p>
 * 2. <b>Control Plane</b> — {@code MasterNotDiscoveredException}, {@code NotMasterException},
 *    and {@code ClusterBlockException} (only when {@code retryable()} is true, i.e. transient
 *    blocks such as no-master; permanent blocks like index-closed are irrecoverable).
 * <p>
 * 3. <b>Resource Plane</b> — {@code CircuitBreakingException} (only when durability is
 *    {@code TRANSIENT}; {@code PERMANENT} durability such as fielddata breaker trips is
 *    irrecoverable), {@code EsRejectedExecutionException} (only when the executor is not shutting
 *    down), {@code VersionConflictEngineException}, and generic {@code TOO_MANY_REQUESTS} /
 *    {@code SERVICE_UNAVAILABLE} status exceptions.
 * <p>
 * 4. <b>Transport Plane</b> — {@code TransportException} and all its subclasses
 *    (connect, receive-timeout, send failures), {@code NodeClosedException},
 *    {@code NoNodeAvailableException}, and {@code ElasticsearchTimeoutException}.
 * <p>
 * {@code TaskCancelledException} and exceptions with irrecoverable REST statuses (NOT_FOUND,
 * BAD_REQUEST, UNAUTHORIZED, FORBIDDEN, GONE, etc.) are always irrecoverable.
 * <p>
 * This classifier is used by
 * {@link org.elasticsearch.xpack.ml.job.task.OpenJobPersistentTasksExecutor OpenJobPersistentTasksExecutor}
 * to decide whether to retry a failed job-open attempt, and by
 * {@link org.elasticsearch.xpack.ml.utils.persistence.ResultsPersisterService ResultsPersisterService}
 * to decide whether to retry a failed result-persistence attempt. Centralising the classification
 * in one place ensures consistent retry behaviour across all ML code paths.
 */
package org.elasticsearch.xpack.ml.utils;
