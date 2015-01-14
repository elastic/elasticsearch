/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.shield;

import org.elasticsearch.Version;

/**
 * This class is used to keep track of changes that we might have to make once we upgrade versions of dependencies, especially elasticsearch core.
 * Every change is listed as a specific assert that trips with a future version of es core, with a meaningful description that explains what needs to be done.
 *
 * NOTE: changes suggested by asserts descriptions may break backwards compatibility. The same shield jar is supposed to work against multiple es core versions,
 * thus if we make a change in shield that requires e.g. es core 1.4.1 it means that the next shield release won't support es core 1.4.0 anymore.
 * In many cases we will just have to bump the version of the assert then, unless we want to break backwards compatibility, but the idea is that this class
 * helps keeping track of this and eventually making changes when needed.
 */
public class VersionCompatibilityChecks {

    static {
        //see https://github.com/elasticsearch/elasticsearch/pull/8634
        assert Version.CURRENT.onOrBefore(Version.V_1_4_2) : "Remove ClusterDiscoveryConfiguration or bump the version, fixed in es core 1.5";
        assert Version.CURRENT.onOrBefore(Version.V_1_4_2) : "Remove TransportProfileUtil class or bump the version, fixed in es core 1.5";
        assert Version.CURRENT.onOrBefore(Version.V_1_4_2) : "Cleanup SecuredMessageChannelHandler class and remove needless code, fixed in es core 1.5";
    }
}
