/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.action.support.master;

import org.elasticsearch.action.ActionResponse;

/**
 * A response to an action which updated the cluster state, but needs to report whether any relevant nodes failed to apply the update. For
 * instance, a {@link org.elasticsearch.action.admin.indices.mapping.put.PutMappingRequest} may update a mapping in the index metadata, but
 * one or more data nodes may fail to acknowledge the new mapping within the ack timeout. If this happens then clients must accept that
 * subsequent requests that rely on the mapping update may return errors from the lagging data nodes.
 * <p>
 * Actions which return a payload-free acknowledgement of success should generally prefer to use {@link ActionResponse.Empty} instead of
 * an implementation of {@link IsAcknowledgedSupplier}, and other listeners should generally prefer {@link Void}.
 */
public interface IsAcknowledgedSupplier {
    boolean isAcknowledged();
}
