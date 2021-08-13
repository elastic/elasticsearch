/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.plugins;

import java.util.Locale;

/**
 * Indicates the type of an Elasticsearch plugin.
 * <p>
 * Elasticsearch plugins come in two flavours: "isolated", which are kept
 * separate from the rest of the Elasticsearch code; and "bootstrap", which
 * take effect when Elasticsearch executes and can modify e.g. JVM
 * behaviour, but do not otherwise hook into the Elasticsearch lifecycle.
 */
public enum PluginType {
    ISOLATED,
    BOOTSTRAP;

    @Override
    public String toString() {
        return this.name().toLowerCase(Locale.ROOT);
    }
}
