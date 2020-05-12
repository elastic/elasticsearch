/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.elasticsearch.gradle.precommit;

import org.elasticsearch.gradle.test.GradleUnitTestCase;

import java.io.File;

public class LicenseAnalyzerTests extends GradleUnitTestCase {

    public void testCanDetectApacheLicense() {
        File licenseFile = new File("/Users/mark/workspaces/elasticsearch/server/licenses/lucene-LICENSE.txt");
        assertEquals("Apache-2.0", LicenseAnalyzer.licenseType(licenseFile).getIdentifier());
    }

    public void testCanDetectBSD2License() {
        File licenseFile = new File("/Users/mark/workspaces/elasticsearch/server/licenses/HdrHistogram-LICENSE.txt");
        assertEquals("BSD-2-Clause", LicenseAnalyzer.licenseType(licenseFile).getIdentifier());
    }

    public void testCanDetectBSD3License() {
        File licenseFile = new File("/Users/mark/workspaces/elasticsearch/x-pack/plugin/watcher/licenses/jakarta.activation-LICENSE.txt");
        assertEquals("BSD-3-Clause", LicenseAnalyzer.licenseType(licenseFile).getIdentifier());
    }

    public void testCanDetectCDDL1_0License() {
        File licenseFile = new File("/Users/mark/workspaces/elasticsearch/plugins/repository-hdfs/licenses/servlet-api-LICENSE.txt");
        assertEquals("CDDL-1.0", LicenseAnalyzer.licenseType(licenseFile).getIdentifier());
    }

    public void testCanDetectCDDL1_1License() {
        File licenseFile = new File("/Users/mark/workspaces/elasticsearch/plugins/discovery-azure-classic/licenses/jaxb-LICENSE.txt");
        assertEquals("CDDL-1.1", LicenseAnalyzer.licenseType(licenseFile).getIdentifier());
    }

    public void testCanDetectICULicense() {
        File licenseFile = new File("/Users/mark/workspaces/elasticsearch/x-pack/plugin/ml/licenses/icu4j-LICENSE.txt");
        assertEquals("ICU", LicenseAnalyzer.licenseType(licenseFile).getIdentifier());
    }

    public void testCanDetectLGPL2_1License() {
        File licenseFile = new File("/Users/mark/workspaces/elasticsearch/x-pack/plugin/core/licenses/unboundid-ldapsdk-LICENSE.txt");
        assertEquals("LGPL-2.1", LicenseAnalyzer.licenseType(licenseFile).getIdentifier());
    }

    public void testCanDetectMITLicense() {
        File licenseFile = new File("/Users/mark/workspaces/elasticsearch/distribution/tools/plugin-cli/licenses/bouncycastle-LICENSE.txt");
        assertEquals("MIT", LicenseAnalyzer.licenseType(licenseFile).getIdentifier());
    }

    public void testCanDetectMPL1_1License() {
        File licenseFile = new File(
            "/Users/mark/workspaces/elasticsearch/plugins/ingest-attachment/licenses/juniversalchardet-LICENSE.txt"
        );
        assertEquals("MPL-1.1", LicenseAnalyzer.licenseType(licenseFile).getIdentifier());
    }

    public void testCanDetectXZLicense() {
        File licenseFile = new File("/Users/mark/workspaces/elasticsearch/plugins/ingest-attachment/licenses/xz-LICENSE.txt");
        assertEquals("XZ", LicenseAnalyzer.licenseType(licenseFile).getIdentifier());
    }
}
