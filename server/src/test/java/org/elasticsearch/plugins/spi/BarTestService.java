/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.plugins.spi;

public class BarTestService implements TestService {
    BarPlugin plugin;

    @SuppressWarnings("unused")
    public BarTestService() {}

    @SuppressWarnings("unused")
    public BarTestService(final BarPlugin plugin) {
        this.plugin = plugin;
    }

    @Override
    public String name() {
        return "aaa";
    }
}
