/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.core.esql.action;

import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.core.Releasable;

/**
 * Response to an ES|QL query request.
 */
public abstract class EsqlQueryResponse extends ActionResponse implements Releasable {

    /** Returns the response. */
    public abstract EsqlResponse response();
}
