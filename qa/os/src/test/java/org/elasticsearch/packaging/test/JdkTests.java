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

package org.elasticsearch.packaging.test;

import org.elasticsearch.packaging.util.Platforms;
import org.elasticsearch.packaging.util.Shell;

import java.util.Arrays;

import static org.junit.Assume.assumeFalse;

public class JdkTests extends PackagingTestCase {

    public void test10Install() throws Exception {
        install();
    }

    public void test90UnsupportedGlibc() throws Exception {
        assumeFalse(Platforms.IS_BUNDLED_JDK_SUPPORTED);
        sh.getEnv().put("JAVA_HOME", ""); // set java home to empty, to ensure any system java home is overridden

        Shell.Result result = runElasticsearchStartCommand(null, false, false);
        assertElasticsearchFailure(result, Arrays.asList("The JDK bundled with Elasticsearch requires glibc >= 2.14"));
    }
}
