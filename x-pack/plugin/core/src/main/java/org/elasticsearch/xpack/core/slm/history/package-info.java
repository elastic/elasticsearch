/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

/**
 * This package contains the utility classes used to persist SLM policy execution results to an internal index.
 *
 * <p>The {@link org.elasticsearch.xpack.core.slm.history.SnapshotLifecycleTemplateRegistry} class is registered as a
 * cluster state listener when the ILM plugin starts up. It executes only on the elected master node, and ensures that a template is
 * configured for the SLM history index, as well as an ILM policy (since the two are always enabled in lock step).
 *
 * <p>The {@link org.elasticsearch.xpack.core.slm.history.SnapshotHistoryItem} is used to encapsulate historical
 * information about a snapshot policy execution. This contains more data than the
 * {@link org.elasticsearch.xpack.core.slm.SnapshotInvocationRecord} since it is a more complete history record
 * stored on disk instead of a low surface area status entry.
 *
 * <p>The {@link org.elasticsearch.xpack.core.slm.history.SnapshotHistoryStore} manages the persistence of the previously
 * mentioned {@link org.elasticsearch.xpack.core.slm.history.SnapshotHistoryItem}. It simply does an asynchronous put
 * operation against the SLM history internal index.
 */
package org.elasticsearch.xpack.core.slm.history;
