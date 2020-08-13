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

package org.elasticsearch.painless;

import static java.util.Collections.singletonMap;

public class HashTests extends ScriptTestCase {
    public void testMD2() {
        String input = "test input";
        Object output = exec("Hash.hash(params.data, 'MD2')", singletonMap("data", input), true);
        assertEquals("f0625c48c038fbf6ae121a339dd48cff", output);
    }

    public void testMD5() {
        String input = "test input";
        Object output = exec("Hash.hash(params.data, 'MD5')", singletonMap("data", input), true);
        assertEquals("5eed650258ee02f6a77c87b748b764ec", output);
    }

    public void testSHA1() {
        String input = "test input";
        Object output = exec("Hash.hash(params.data, 'SHA-1')", singletonMap("data", input), true);
        assertEquals("49883b34e5a0f48224dd6230f471e9dc1bdbeaf5", output);
    }

    public void testSHA256() {
        String input = "test input";
        Object output = exec("Hash.hash(params.data, 'SHA-256')", singletonMap("data", input), true);
        assertEquals("9dfe6f15d1ab73af898739394fd22fd72a03db01834582f24bb2e1c66c7aaeae", output);
    }

    public void testSHA384() {
        String input = "test input";
        Object output = exec("Hash.hash(params.data, 'SHA-384')", singletonMap("data", input), true);
        assertEquals("1d8e2db6723d79d7cb2d2d9df903c206c6381120263ef0bec9db076b4204f73ff931658276ae7e19f245963815ae7eed", output);
    }

    public void testSHA512() {
        String input = "test input";
        Object output = exec("Hash.hash(params.data, 'SHA-512')", singletonMap("data", input), true);
        assertEquals("40aa1b203c9d8ee150b21c3c7cda8261492e5420c5f2b9f7380700e094c303b48e62f319c1da0e32eb40d113c5f1749cc61aeb499167890ab82f2cc9bb706971", output);
    }

}