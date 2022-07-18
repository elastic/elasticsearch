/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.sql.action.compute.exchange;

import org.elasticsearch.xpack.sql.action.compute.Page;

public class PassthroughExchanger implements Exchanger {

    private final ExchangeSource exchangeSource;

    public PassthroughExchanger(ExchangeSource exchangeSource) {
        this.exchangeSource = exchangeSource;
    }

    @Override
    public void accept(Page page) {
        exchangeSource.addPage(page);
    }

//    @Override
//    public void finish() {
//        exchangeSource.finish();
//    }
}
