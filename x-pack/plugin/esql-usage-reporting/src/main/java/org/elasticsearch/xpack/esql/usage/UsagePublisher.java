/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.usage;

import org.elasticsearch.core.Releasable;

import java.util.List;

/**
 * The seam where records leave the node, the sink. One call delivers one batch of at most {@code batch_size} records and may
 * block, so it is called only from the reporter thread, never from a query thread. An implementation never changes the id of a
 * record, and isolates a bad record itself (for example by splitting the batch in halves until one record is left) reporting it as
 * {@link PublishResult.FailureClass#INVALID}, so that one bad record does not hold back the others. Other sinks, such as a local
 * store for self-managed deployments, can implement it later without touching the rest.
 * <p>
 * It is {@link Releasable} because a real publisher holds an HTTP client. The plugin closes it, after the reporter, whose final
 * flush still needs it.
 * <p>
 * A result that accepts nothing and reports no failure ends the reporter's cycle just as a failure does: the reporter must not
 * ask for the same batch again.
 */
public interface UsagePublisher extends Releasable {

    PublishResult publish(List<UsageRecord> records);
}
