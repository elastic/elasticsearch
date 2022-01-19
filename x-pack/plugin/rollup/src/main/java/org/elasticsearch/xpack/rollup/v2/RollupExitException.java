/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.rollup.v2;

import org.elasticsearch.ElasticsearchException;

public class RollupExitException extends ElasticsearchException {
    public RollupExitException(String msg, Exception e) {
        super(msg, e);
    }
}
