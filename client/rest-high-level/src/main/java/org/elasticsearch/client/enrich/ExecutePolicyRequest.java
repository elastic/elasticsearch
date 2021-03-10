/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
package org.elasticsearch.client.enrich;

import org.elasticsearch.client.Validatable;

public final class ExecutePolicyRequest implements Validatable {

    private final String name;
    private Boolean waitForCompletion;

    public ExecutePolicyRequest(String name) {
        this.name = name;
    }

    public String getName() {
        return name;
    }

    public Boolean getWaitForCompletion() {
        return waitForCompletion;
    }

    public void setWaitForCompletion(boolean waitForCompletion) {
        this.waitForCompletion = waitForCompletion;
    }
}
