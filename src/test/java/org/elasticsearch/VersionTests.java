/*
 * Licensed to ElasticSearch and Shay Banon under one
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

package org.elasticsearch;

import org.elasticsearch.test.ElasticsearchTestCase;
import org.junit.Test;

import static org.elasticsearch.Version.V_0_20_0;
import static org.elasticsearch.Version.V_0_90_0;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;

public class VersionTests extends ElasticsearchTestCase {

    @Test
    public void testVersions() throws Exception {
        assertThat(V_0_20_0.before(V_0_90_0), is(true));
        assertThat(V_0_20_0.before(V_0_20_0), is(false));
        assertThat(V_0_90_0.before(V_0_20_0), is(false));

        assertThat(V_0_20_0.onOrBefore(V_0_90_0), is(true));
        assertThat(V_0_20_0.onOrBefore(V_0_20_0), is(true));
        assertThat(V_0_90_0.onOrBefore(V_0_20_0), is(false));

        assertThat(V_0_20_0.after(V_0_90_0), is(false));
        assertThat(V_0_20_0.after(V_0_20_0), is(false));
        assertThat(V_0_90_0.after(V_0_20_0), is(true));

        assertThat(V_0_20_0.onOrAfter(V_0_90_0), is(false));
        assertThat(V_0_20_0.onOrAfter(V_0_20_0), is(true));
        assertThat(V_0_90_0.onOrAfter(V_0_20_0), is(true));
    }

}