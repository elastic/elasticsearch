/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.shield;

import org.elasticsearch.Version;
import org.elasticsearch.test.ESTestCase;

import static org.hamcrest.CoreMatchers.is;

/**
 * This class is used to keep track of changes that we might have to make once we upgrade versions of dependencies, especially
 * elasticsearch core.
 * Every change is listed as a specific assert that trips with a future version of es core, with a meaningful description that explains
 * what needs to be done.
 * <p>
 * For each assertion we should have one or more corresponding TODOs in the code points that require changes, and also a link to the
 * issue that applies the
 * required fixes upstream.
 * <p>
 * NOTE: changes suggested by asserts descriptions may break backwards compatibility. The same shield jar is supposed to work against
 * multiple es core versions,
 * thus if we make a change in shield that requires e.g. es core 1.4.1 it means that the next shield release won't support es core 1.4.0
 * anymore.
 * In many cases we will just have to bump the version of the assert then, unless we want to break backwards compatibility, but the idea
 * is that this class
 * helps keeping track of this and eventually making changes when needed.
 */
public class VersionCompatibilityTests extends ESTestCase {
    public void testCompatibility() {
        /**
         * see https://github.com/elasticsearch/elasticsearch/issues/9372 {@link ShieldLicensee}
         * Once es core supports merging cluster level custom metadata (licenses in our case), the tribe node will see some license
         * coming from the tribe and everything will be ok.
         *
         */
        assertThat("Remove workaround in LicenseService class when es core supports merging cluster level custom metadata",
                Version.CURRENT.onOrBefore(Version.V_5_0_0_alpha2), is(true));
    }
}
