/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.core.ilm.action;

import org.elasticsearch.action.ActionType;
import org.elasticsearch.action.support.master.AcknowledgedResponse;

public enum ILMActions {
    ;
    public static final ActionType<AcknowledgedResponse> START = ActionType.acknowledgedResponse("cluster:admin/ilm/start");
    public static final ActionType<AcknowledgedResponse> STOP = ActionType.acknowledgedResponse("cluster:admin/ilm/stop");
    public static final ActionType<AcknowledgedResponse> RETRY = ActionType.acknowledgedResponse("indices:admin/ilm/retry");
    public static final ActionType<AcknowledgedResponse> MOVE_TO_STEP = ActionType.acknowledgedResponse("cluster:admin/ilm/_move/post");
    public static final ActionType<AcknowledgedResponse> PUT = ActionType.acknowledgedResponse("cluster:admin/ilm/put");
}
