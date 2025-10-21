/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.multiproject;

import org.elasticsearch.client.Request;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.test.rest.ESRestTestCase;
import org.junit.After;

import java.io.IOException;

public abstract class MultiProjectRestTestCase extends ESRestTestCase {

    @Override
    protected boolean shouldConfigureProjects() {
        return false;
    }

    @After
    public void removeNonDefaultProjects() throws IOException {
        if (preserveClusterUponCompletion() == false) {
            cleanUpProjects();
        }
    }

    protected static Request setRequestProjectId(Request request, String projectId) {
        RequestOptions.Builder options = request.getOptions().toBuilder();
        options.removeHeader(Task.X_ELASTIC_PROJECT_ID_HTTP_HEADER);
        options.addHeader(Task.X_ELASTIC_PROJECT_ID_HTTP_HEADER, projectId);
        request.setOptions(options);
        return request;
    }

}
