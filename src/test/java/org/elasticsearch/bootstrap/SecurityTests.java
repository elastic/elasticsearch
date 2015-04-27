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

import org.elasticsearch.test.ElasticsearchTestCase;

import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.util.Collections;

public class SecurityTests extends ElasticsearchTestCase {
    
    /** backslash escaping (e.g. windows paths) */
    public void testEncode() {
        assertEquals("c:\\\\foobar", Security.encode("c:\\foobar"));
    }
    
    /** test template processing */
    public void testTemplateProcessing() throws Exception {
        Path path = createTempDir();
        
        byte results[] = Security.createPermissions(Collections.singleton(path));
        String unicode = new String(results, StandardCharsets.UTF_8);
        // try not to make this test too fragile or useless
        assertTrue(unicode.contains("grant {"));
        assertTrue(unicode.contains(Security.encode(path)));
        assertTrue(unicode.contains("read"));
        assertTrue(unicode.contains("write"));
    }
    
}
