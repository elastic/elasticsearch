/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.compat;

import org.elasticsearch.Version;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.plugins.RestCompatibilityPlugin;

public class CompatRestRequestPlugin extends Plugin implements RestCompatibilityPlugin {

    @Override
    public Version minimumRestCompatibilityVersion() {
        return Version.fromString(Version.CURRENT.major-1+".0.0");
    }
}
