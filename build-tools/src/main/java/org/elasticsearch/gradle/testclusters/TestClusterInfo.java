/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */
package org.elasticsearch.gradle.testclusters;

import java.io.File;
import java.util.List;

public class TestClusterInfo {
    private final List<String> allHttpSocketURI;
    private final List<String> allTransportPortURI;
    private final List<File> auditLogs;

    public TestClusterInfo(List<String> allHttpSocketURI, List<String> allTransportPortURI, List<File> auditLogs) {
        this.allHttpSocketURI = allHttpSocketURI;
        this.allTransportPortURI = allTransportPortURI;
        this.auditLogs = auditLogs;
    }

    public List<String> getAllHttpSocketURI() {
        return allHttpSocketURI;
    }

    public List<String> getAllTransportPortURI() {
        return allTransportPortURI;
    }

    public List<File> getAuditLogs() {
        return auditLogs;
    }
}
