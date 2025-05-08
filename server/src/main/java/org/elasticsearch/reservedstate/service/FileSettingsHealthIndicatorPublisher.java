/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.reservedstate.service;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.master.AcknowledgedResponse;
import org.elasticsearch.reservedstate.service.FileSettingsService.FileSettingsHealthTracker;

/**
 * Used by {@link FileSettingsHealthTracker} to send health info to the health node.
 */
public interface FileSettingsHealthIndicatorPublisher {
    void publish(FileSettingsService.FileSettingsHealthInfo info, ActionListener<AcknowledgedResponse> actionListener);
}
