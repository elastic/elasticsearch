/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.client.indices;

import org.elasticsearch.client.TimedRequest;

public class DeleteComponentTemplateRequest extends TimedRequest {

    private final String name;

    public DeleteComponentTemplateRequest(String name) {
        this.name = name;
    }

    public String getName() {
        return name;
    }
}
