/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.shield.authc.support;

/**
 *
 */
public interface RefreshListener {

    static final RefreshListener NOOP = new RefreshListener() {
        @Override
        public void onRefresh() {
        }
    };

    void onRefresh();

}
