/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.compat;

import org.elasticsearch.Version;
import org.elasticsearch.plugins.spi.CompatibleApiVersionProvider;

//TODO I don't think we want SPI approach. to load spi with ServiceLoader we need to fetch a plugin with the same classloader
// that means we can get the minimumRestCompatibilityVersion from the plugin itself
public class PreviousVersionCompatibleApiProvider implements CompatibleApiVersionProvider {

    @Override
    public Version minimumRestCompatibilityVersion() {
        return Version.fromString(Version.CURRENT.major-1+"0.0");
    }

}
