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
    public static final ActionType<AcknowledgedResponse> START = new ActionType<>("cluster:admin/ilm/start");
    public static final ActionType<AcknowledgedResponse> STOP = new ActionType<>("cluster:admin/ilm/stop");
    public static final ActionType<AcknowledgedResponse> RETRY = new ActionType<>("indices:admin/ilm/retry");
    public static final ActionType<AcknowledgedResponse> MOVE_TO_STEP = new ActionType<>("cluster:admin/ilm/_move/post");
    public static final ActionType<AcknowledgedResponse> PUT = new ActionType<>("cluster:admin/ilm/put");
}
