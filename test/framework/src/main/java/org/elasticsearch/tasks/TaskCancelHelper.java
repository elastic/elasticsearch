/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.tasks;

/**
 * Helper class to expose {@link CancellableTask#cancel} for use in tests.
 */
public class TaskCancelHelper {
    private TaskCancelHelper() {}

    public static void cancel(CancellableTask task, String reason) {
        task.cancel(reason);
    }
}
