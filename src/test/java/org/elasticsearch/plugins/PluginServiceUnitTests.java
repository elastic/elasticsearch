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

package org.elasticsearch.plugins;

import org.elasticsearch.test.ElasticsearchTestCase;
import org.junit.Test;

import java.net.MalformedURLException;
import java.net.URL;
import java.util.Arrays;
import java.util.List;

import static org.hamcrest.Matchers.is;

public class PluginServiceUnitTests extends ElasticsearchTestCase {

    @Test
    public void testThatPluginUrlsAreUniqueAndSorted() throws MalformedURLException {
        List<URL> urls = Arrays.asList(new URL[]{ new URL("http://bar"), new URL("http://bbb"), new URL("http://bar") });
        Object[] uniqueUrls = PluginsService.getUniqueSortedUrls(urls).toArray();
        assertThat(uniqueUrls, is(new Object[]{ new URL("http://bar"), new URL("http://bbb") }));
    }
}
