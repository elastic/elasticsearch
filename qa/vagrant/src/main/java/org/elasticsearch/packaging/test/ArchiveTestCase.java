/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.elasticsearch.packaging.test;

import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.FixMethodOrder;
import org.junit.Test;
import org.junit.runners.MethodSorters;

import org.elasticsearch.packaging.util.Distribution;
import org.elasticsearch.packaging.util.Installation;

import static org.elasticsearch.packaging.util.Cleanup.cleanEverything;
import static org.elasticsearch.packaging.util.Archives.installArchive;
import static org.elasticsearch.packaging.util.Archives.verifyArchiveInstallation;
import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assume.assumeThat;

/**
 * Tests that apply to the archive distributions (tar, zip). To add a case for a distribution, subclass and
 * override {@link ArchiveTestCase#distribution()}. These tests should be the same across all archive distributions
 */
@FixMethodOrder(MethodSorters.NAME_ASCENDING)
public abstract class ArchiveTestCase {

    private static Installation installation;

    /** The {@link Distribution} that should be tested in this case */
    protected abstract Distribution distribution();

    @BeforeClass
    public static void cleanup() {
        installation = null;
        cleanEverything();
    }

    @Before
    public void onlyCompatibleDistributions() {
        assumeThat(distribution().packaging.compatible, is(true));
    }

    @Test
    public void test10Install() {
        installation = installArchive(distribution());
        verifyArchiveInstallation(installation, distribution());
    }
}
