/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.gradle.internal;

import org.gradle.api.model.ObjectFactory;
import org.gradle.api.provider.ListProperty;

public class DistributionArchiveCheckExtension {

    ListProperty<String> expectedMlLicenses;

    public DistributionArchiveCheckExtension(ObjectFactory factory) {
        this.expectedMlLicenses = factory.listProperty(String.class);
    }
}
