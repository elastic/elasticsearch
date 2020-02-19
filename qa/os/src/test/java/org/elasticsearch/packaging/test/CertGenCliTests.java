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

import org.apache.http.client.fluent.Request;
import org.elasticsearch.packaging.util.Distribution;
import org.elasticsearch.packaging.util.FileUtils;
import org.elasticsearch.packaging.util.Platforms;
import org.elasticsearch.packaging.util.ServerUtils;
import org.elasticsearch.packaging.util.Shell;
import org.junit.Before;
import org.junit.BeforeClass;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;

import static com.carrotsearch.randomizedtesting.RandomizedTest.assumeFalse;
import static org.elasticsearch.packaging.util.FileMatcher.Fileness.File;
import static org.elasticsearch.packaging.util.FileMatcher.file;
import static org.elasticsearch.packaging.util.FileMatcher.p600;
import static org.elasticsearch.packaging.util.FileUtils.append;
import static org.elasticsearch.packaging.util.FileUtils.escapePath;
import static org.elasticsearch.packaging.util.FileUtils.getTempDir;
import static org.hamcrest.CoreMatchers.containsString;
import static org.junit.Assume.assumeTrue;

public class CertGenCliTests extends PackagingTestCase {
    private static final Path instancesFile = getTempDir().resolve("instances.yml");
    private static final Path certificatesFile = getTempDir().resolve("certificates.zip");

    @Before
    public void filterDistros() {
        assumeTrue("only default distro", distribution.flavor == Distribution.Flavor.DEFAULT);
        assumeTrue("no docker", distribution.packaging != Distribution.Packaging.DOCKER);
    }

    @BeforeClass
    public static void cleanupFiles() {
        FileUtils.rm(instancesFile, certificatesFile);
    }

    public void test10Install() throws Exception {
        install();
    }

    public void test20Help() throws Exception {
        Shell.Result result = installation.executables().certgenTool.run("--help");
        assertThat(result.stdout, containsString("Simplifies certificate creation"));
    }

    public void test30Generate() throws Exception {
        Files.write(instancesFile, Arrays.asList(
            "instances:",
            "  - name: \"mynode\"",
            "    ip:",
            "      - \"127.0.0.1\""));

        installation.executables().certgenTool.run("--in " + instancesFile + " --out " + certificatesFile);

        String owner = installation.getOwner();
        assertThat(certificatesFile, file(File, owner, owner, p600));
    }

    public void test31ExtractCerts() throws Exception {
        // windows 2012 r2 has powershell 4.0, which lacks Expand-Archive
        assumeFalse(Platforms.OS_NAME.equals("Windows Server 2012 R2"));

        Path certsDir = installation.config("certs");
        sh.extractZip(certificatesFile, certsDir);

        Path caDir = certsDir.resolve("ca");
        assertThat(caDir.resolve("ca.key"), file(File, null, null, null));
        assertThat(caDir.resolve("ca.crt"), file(File, null, null, null));

        Path nodeDir = certsDir.resolve("mynode");
        assertThat(nodeDir.resolve("mynode.key"), file(File, null, null, null));
        assertThat(nodeDir.resolve("mynode.crt"), file(File, null, null, null));

        FileUtils.cp(certsDir, installation.config("certs"));
    }

    public void test40RunWithCert() throws Exception {
        // windows 2012 r2 has powershell 4.0, which lacks Expand-Archive
        assumeFalse(Platforms.OS_NAME.equals("Windows Server 2012 R2"));
        
        append(installation.config("elasticsearch.yml"), String.join("\n",
            "node.name: mynode",
            "xpack.security.transport.ssl.key: " + escapePath(installation.config("certs/mynode/mynode.key")),
            "xpack.security.transport.ssl.certificate: " + escapePath(installation.config("certs/mynode/mynode.crt")),
            "xpack.security.transport.ssl.certificate_authorities: [\"" + escapePath(installation.config("certs/ca/ca.crt")) + "\"]",
            "xpack.security.http.ssl.key: " + escapePath(installation.config("certs/mynode/mynode.key")),
            "xpack.security.http.ssl.certificate: "+ escapePath(installation.config("certs/mynode/mynode.crt")),
            "xpack.security.http.ssl.certificate_authorities: [\"" + escapePath(installation.config("certs/ca/ca.crt")) + "\"]",
            "xpack.security.transport.ssl.enabled: true",
            "xpack.security.http.ssl.enabled: true"));

        assertWhileRunning(() -> {
            ServerUtils.makeRequest(Request.Get("https://127.0.0.1:9200"), null, null, installation.config("certs/ca/ca.crt"));
        });
    }
}
