/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.gradle.internal.test;

/**
 * Any object that can produce an accompanying stop task, meant to tear down
 * a previously instantiated service.
 */
public interface Fixture {

    /** A task which will stop this fixture. This should be used as a finalizedBy for any tasks that use the fixture. */
    Object getStopTask();

}
