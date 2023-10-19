/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.test.rest;

/**
 * This class is for the test framework consumption (ESRestTestCase and derived classes). It exposes features available on the cluster
 * under test (current or an older version in BwC/upgrade/mixed tests), so that tests can take different actions/decisions based on their
 * availability (or not).
 * Features exposed will be all and only features needed by the framework.
 */
public interface ESRestTestNodesFeatures {
    boolean hasFeature(String featureName);
}
