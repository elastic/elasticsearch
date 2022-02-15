/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.enrich.action;

import org.elasticsearch.action.ActionType;
import org.elasticsearch.index.reindex.BulkByScrollResponse;

/**
 * This class exists only to support {@link TransportEnrichReindexAction}.
 */
public class EnrichReindexAction extends ActionType<BulkByScrollResponse> {

    public static final String NAME = "cluster:admin/xpack/enrich/reindex";
    public static final EnrichReindexAction INSTANCE = new EnrichReindexAction();

    private EnrichReindexAction() {
        super(NAME, BulkByScrollResponse::new);
    }
}
