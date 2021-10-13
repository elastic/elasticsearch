/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.client.transform;

import org.elasticsearch.client.Validatable;

import java.util.Objects;

public class UpgradeTransformsRequest implements Validatable {

    private Boolean dryRun;

    public UpgradeTransformsRequest() {}

    public Boolean isDryRun() {
        return dryRun;
    }

    /**
     * Whether to only check for an upgrade without taking action
     *
     * @param dryRun {@code true} will only check for upgrades
     */
    public void setDryRun(boolean dryRun) {
        this.dryRun = dryRun;
    }

    @Override
    public int hashCode() {
        return Objects.hash(dryRun);
    }

    @Override
    public boolean equals(Object obj) {
        if (obj == null) {
            return false;
        }
        if (getClass() != obj.getClass()) {
            return false;
        }
        UpgradeTransformsRequest other = (UpgradeTransformsRequest) obj;
        return Objects.equals(dryRun, other.dryRun);
    }

}
