/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.action.admin.indices;

import org.elasticsearch.action.ActionFuture;
import org.elasticsearch.action.admin.indices.create.CreateIndexResponse;
import org.elasticsearch.action.admin.indices.shrink.ResizeRequest;
import org.elasticsearch.action.admin.indices.shrink.ResizeType;
import org.elasticsearch.action.admin.indices.shrink.TransportResizeAction;
import org.elasticsearch.common.settings.Settings;

import static org.elasticsearch.test.ESIntegTestCase.client;
import static org.elasticsearch.test.ESTestCase.TEST_REQUEST_TIMEOUT;

public enum ResizeIndexTestUtils {
    ;

    public static ResizeRequest resizeRequest(ResizeType resizeType, String sourceIndex, String targetIndex, Settings.Builder settings) {
        final var resizeRequest = new ResizeRequest(TEST_REQUEST_TIMEOUT, TEST_REQUEST_TIMEOUT, resizeType, sourceIndex, targetIndex);
        resizeRequest.setTargetIndexSettings(settings);
        return resizeRequest;
    }

    public static ActionFuture<CreateIndexResponse> executeResize(
        ResizeType resizeType,
        String sourceIndex,
        String targetIndex,
        Settings.Builder settings
    ) {
        return client().execute(TransportResizeAction.TYPE, resizeRequest(resizeType, sourceIndex, targetIndex, settings));
    }
}
