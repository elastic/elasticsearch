/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.inference.services.elser;

import org.elasticsearch.inference.ServiceSettings;
import org.elasticsearch.inference.TaskSettings;
import org.elasticsearch.inference.TaskType;
import org.elasticsearch.inference.services.InferenceService;

public class ElserService implements InferenceService {

    public static final String NAME = "elser";

    @Override
    public String name() {
        return NAME;
    }

    @Override
    public ServiceSettings serviceSettings() {
        return null;
    }

    @Override
    public TaskSettings taskSettings(TaskType taskType) {
        return null;
    }
}
