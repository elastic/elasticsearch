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

package org.elasticsearch.cloud.aws;

import com.amazonaws.ClientConfiguration;
import org.elasticsearch.plugin.discovery.ec2.Ec2DiscoveryPlugin;
import org.elasticsearch.test.ESTestCase;
import org.junit.BeforeClass;

import static org.hamcrest.CoreMatchers.is;

public class AWSSignersTests extends ESTestCase {

    /**
     * Starts Ec2DiscoveryPlugin. It's a workaround when you run test from IntelliJ. Otherwise it generates
     * java.security.AccessControlException: access denied ("java.lang.RuntimePermission" "accessDeclaredMembers")
     */
    @BeforeClass
    public static void instantiatePlugin() {
        new Ec2DiscoveryPlugin();
    }

    public void testSigners() {
        assertThat(signerTester(null), is(false));
        assertThat(signerTester("QueryStringSignerType"), is(true));
        assertThat(signerTester("AWS3SignerType"), is(true));
        assertThat(signerTester("AWS4SignerType"), is(true));
        assertThat(signerTester("NoOpSignerType"), is(true));
        assertThat(signerTester("UndefinedSigner"), is(false));

        assertThat(signerTester("S3SignerType"), is(false));
        assertThat(signerTester("AWSS3V4SignerType"), is(false));

        ClientConfiguration configuration = new ClientConfiguration();
        AwsSigner.configureSigner("AWS4SignerType", configuration);
        assertEquals(configuration.getSignerOverride(), "AWS4SignerType");
        AwsSigner.configureSigner("AWS3SignerType", configuration);
        assertEquals(configuration.getSignerOverride(), "AWS3SignerType");
    }

    /**
     * Test a signer configuration
     * @param signer signer name
     * @return true if successful, false otherwise
     */
    private boolean signerTester(String signer) {
        try {
            AwsSigner.validateSignerType(signer);
            return true;
        } catch (IllegalArgumentException e) {
            return false;
        }
    }
}
