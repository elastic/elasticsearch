/*
 * Licensed to Elastic Search and Shay Banon under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. Elastic Search licenses this
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

package org.elasticsearch.rest.util;

import org.elasticsearch.rest.support.RestUtils;
import org.testng.annotations.Test;

import java.util.Map;

import static org.elasticsearch.common.collect.Maps.*;
import static org.hamcrest.MatcherAssert.*;
import static org.hamcrest.Matchers.*;

/**
 * @author kimchy (shay.banon)
 */
@Test
public class RestUtilsTests {

    @Test
    public void testDecodeQueryString() {
        Map<String, String> params = newHashMap();

        String uri = "something?test=value";
        RestUtils.decodeQueryString(uri, uri.indexOf('?') + 1, params);
        assertThat(params.size(), equalTo(1));
        assertThat(params.get("test"), equalTo("value"));

        params.clear();
        uri = "something?test=value&test1=value1";
        RestUtils.decodeQueryString(uri, uri.indexOf('?') + 1, params);
        assertThat(params.size(), equalTo(2));
        assertThat(params.get("test"), equalTo("value"));
        assertThat(params.get("test1"), equalTo("value1"));

        params.clear();
        uri = "something";
        RestUtils.decodeQueryString(uri, uri.indexOf('?') + 1, params);
        assertThat(params.size(), equalTo(0));
    }
}
