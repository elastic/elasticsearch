/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.sql.action;

import org.elasticsearch.core.TimeValue;
import org.elasticsearch.xpack.sql.proto.CoreProtocol;

import static org.elasticsearch.xpack.sql.action.ProtoShim.fromProto;

/**
 * Sql protocol defaults used for sharding constants across the code base both in server and clients.
 * Expands and converts some constants from the sql-proto CoreProtocol class which has NO dependencies to ES.
 */
public final class Protocol extends CoreProtocol {

    /**
     * Global choice for the default fetch size.
     */
    public static final TimeValue REQUEST_TIMEOUT = fromProto(CoreProtocol.REQUEST_TIMEOUT);
    public static final TimeValue PAGE_TIMEOUT = fromProto(CoreProtocol.PAGE_TIMEOUT);

    public static final TimeValue DEFAULT_KEEP_ALIVE = fromProto(CoreProtocol.DEFAULT_KEEP_ALIVE);
    public static final TimeValue DEFAULT_WAIT_FOR_COMPLETION_TIMEOUT = fromProto(CoreProtocol.DEFAULT_WAIT_FOR_COMPLETION_TIMEOUT);
    public static final TimeValue MIN_KEEP_ALIVE = fromProto(CoreProtocol.MIN_KEEP_ALIVE);
}
