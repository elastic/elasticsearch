/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.external.http.sender;

import org.elasticsearch.common.util.concurrent.AbstractRunnable;

class NoopTask extends AbstractRunnable {

    @Override
    public void onFailure(Exception e) {}

    @Override
    protected void doRun() throws Exception {}
}
