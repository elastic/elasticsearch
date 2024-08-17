/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.parser;

import org.elasticsearch.Build;

class EsqlConfig {

    // versioning information
    boolean devVersion = Build.current().isSnapshot();

    public void setDevVersion(boolean dev) {
        this.devVersion = dev;
    }

    // not great because other grammar parser (Kibana) need the implement this method
    boolean hasFeature(String featureName) {
        return false;
    }
}
