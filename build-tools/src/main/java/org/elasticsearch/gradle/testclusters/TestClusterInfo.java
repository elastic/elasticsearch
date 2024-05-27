/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.gradle.testclusters;

import java.util.List;

public class TestClusterInfo {
    private final List<String> allHttpSocketURI;
    private final List<String> allTransportPortURI;

    public TestClusterInfo(List<String> allHttpSocketURI, List<String> allTransportPortURI) {

        this.allHttpSocketURI = allHttpSocketURI;
        this.allTransportPortURI = allTransportPortURI;
    }

    public List<String> getAllHttpSocketURI() {
        return allHttpSocketURI;
    }

    public List<String> getAllTransportPortURI() {
        return allTransportPortURI;
    }
}
