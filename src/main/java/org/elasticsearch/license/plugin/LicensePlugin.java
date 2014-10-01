/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.license.plugin;

import org.elasticsearch.plugins.AbstractPlugin;

//TODO: plugin hooks
public class LicensePlugin extends AbstractPlugin {

    @Override
    public String name() {
        return "license";
    }

    @Override
    public String description() {
        return "Internal Elasticsearch Licensing Plugin";
    }
}
