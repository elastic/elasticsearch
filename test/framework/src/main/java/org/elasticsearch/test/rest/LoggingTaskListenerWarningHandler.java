/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.test.rest;

import org.elasticsearch.client.WarningsHandler;
import org.elasticsearch.core.UpdateForV9;
import org.elasticsearch.tasks.LoggingTaskListener;

import java.util.List;

@UpdateForV9 // this warning won't be emitted in v9 so this can be removed
public class LoggingTaskListenerWarningHandler implements WarningsHandler {

    public static final LoggingTaskListenerWarningHandler INSTANCE = new LoggingTaskListenerWarningHandler();

    private LoggingTaskListenerWarningHandler() {}

    @Override
    public boolean warningsShouldFailRequest(List<String> warnings) {
        return warnings.stream()
            .anyMatch(
                s -> s.startsWith(
                    "Logging the completion of a task using ["
                        + LoggingTaskListener.class.getCanonicalName()
                        + "] is deprecated and will be removed in a future version."
                ) == false
            );
    }
}
