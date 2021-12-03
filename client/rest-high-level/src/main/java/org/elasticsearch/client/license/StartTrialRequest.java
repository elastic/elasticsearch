/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.client.license;

import org.elasticsearch.client.Validatable;
import org.elasticsearch.core.Nullable;

public class StartTrialRequest implements Validatable {

    private final boolean acknowledge;
    private final String licenseType;

    public StartTrialRequest() {
        this(false);
    }

    public StartTrialRequest(boolean acknowledge) {
        this(acknowledge, null);
    }

    public StartTrialRequest(boolean acknowledge, @Nullable String licenseType) {
        this.acknowledge = acknowledge;
        this.licenseType = licenseType;
    }

    public boolean isAcknowledge() {
        return acknowledge;
    }

    public String getLicenseType() {
        return licenseType;
    }
}
