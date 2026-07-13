/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.entitlement.qa.test;

import java.io.IOException;
import java.util.List;

@SuppressWarnings({ "unused" /* called via reflection */ })
class OperatingSystemActions {

    static void processBuilder_start() throws IOException {
        new ProcessBuilder("").start();
    }

    static void processBuilder_startPipeline() throws IOException {
        ProcessBuilder.startPipeline(List.of());
    }

    private OperatingSystemActions() {}
}
