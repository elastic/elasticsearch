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

package org.elasticsearch.bootstrap;

import org.elasticsearch.common.io.PathUtils;
import org.elasticsearch.test.ElasticsearchTestCase;
import org.junit.Test;

import java.nio.file.Path;
import java.nio.file.PathMatcher;

public class BootstrapTests extends ElasticsearchTestCase {

    @Test
    public void testHasLibExtension() {
        PathMatcher matcher = PathUtils.getDefaultFileSystem().getPathMatcher(Bootstrap.PLUGIN_LIB_PATTERN);

        Path p = PathUtils.get("path", "to", "plugin.jar");
        assertTrue(matcher.matches(p));

        p = PathUtils.get("path", "to", "plugin.zip");
        assertTrue(matcher.matches(p));

        p = PathUtils.get("path", "to", "plugin.tar.gz");
        assertFalse(matcher.matches(p));

        p = PathUtils.get("path", "to", "plugin");
        assertFalse(matcher.matches(p));
    }
}
