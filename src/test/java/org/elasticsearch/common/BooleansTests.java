package org.elasticsearch.common;
/*
 * Licensed to ElasticSearch under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. ElasticSearch licenses this
 * file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
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


import org.elasticsearch.test.ElasticsearchTestCase;
import org.hamcrest.Matchers;
import org.junit.Test;

public class BooleansTests extends ElasticsearchTestCase {

    @Test
    public void testIsBoolean() {
        String[] booleans = new String[]{"true", "false", "on", "off", "yes", "no", "0", "1"};
        String[] notBooleans = new String[]{"11", "00", "sdfsdfsf", "F", "T"};

        for (String b : booleans) {
            String t = "prefix" + b + "suffix";
            assertThat("failed to recognize [" + b + "] as boolean", Booleans.isBoolean(t.toCharArray(), "prefix".length(), b.length()), Matchers.equalTo(true));
        }

        for (String nb : notBooleans) {
            String t = "prefix" + nb + "suffix";
            assertThat("recognized [" + nb + "] as boolean", Booleans.isBoolean(t.toCharArray(), "prefix".length(), nb.length()), Matchers.equalTo(false));
        }
    }
}
