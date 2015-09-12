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
package org.elasticsearch.common.util;

import org.elasticsearch.test.ESTestCase;
import org.junit.Test;

import java.net.URI;

public class URIPatternTests extends ESTestCase {

    @Test
    public void testURIPattern() throws Exception {
        assertTrue(new URIPattern("http://test.local/").match(new URI("http://test.local/")));
        assertFalse(new URIPattern("http://test.local/somepath").match(new URI("http://test.local/")));
        assertTrue(new URIPattern("http://test.local/somepath").match(new URI("http://test.local/somepath")));
        assertFalse(new URIPattern("http://test.local/somepath").match(new URI("http://test.local/somepath/more")));
        assertTrue(new URIPattern("http://test.local/somepath/*").match(new URI("http://test.local/somepath/more")));
        assertTrue(new URIPattern("http://test.local/somepath/*").match(new URI("http://test.local/somepath/more/andmore")));
        assertTrue(new URIPattern("http://test.local/somepath/*").match(new URI("http://test.local/somepath/more/andmore/../bitmore")));
        assertFalse(new URIPattern("http://test.local/somepath/*").match(new URI("http://test.local/somepath/../more")));
        assertFalse(new URIPattern("http://test.local/somepath/*").match(new URI("http://test.local/")));
        assertFalse(new URIPattern("http://test.local/somepath/*").match(new URI("https://test.local/somepath/more")));
        assertFalse(new URIPattern("http://test.local:1234/somepath/*").match(new URI("http://test.local/somepath/more")));
        assertFalse(new URIPattern("http://test.local:1234/somepath/*").match(new URI("http://test.local/somepath/more")));
        assertTrue(new URIPattern("http://test.local:1234/somepath/*").match(new URI("http://test.local:1234/somepath/more")));
        assertTrue(new URIPattern("http://*.local:1234/somepath/*").match(new URI("http://foobar.local:1234/somepath/more")));
        assertFalse(new URIPattern("http://*.local:1234/somepath/*").match(new URI("http://foobar.local:2345/somepath/more")));
        assertTrue(new URIPattern("http://*.local:*/somepath/*").match(new URI("http://foobar.local:2345/somepath/more")));
        assertFalse(new URIPattern("http://*.local:*/somepath/*").match(new URI("http://foobar.local:2345/somepath/more?par=val")));
        assertTrue(new URIPattern("http://*.local:*/somepath/*?*").match(new URI("http://foobar.local:2345/somepath/more?par=val")));
        assertFalse(new URIPattern("http://*.local:*/somepath/*?*").match(new URI("http://foobar.local:2345/somepath/more?par=val#frag")));
        assertTrue(new URIPattern("http://*.local:*/somepath/*?*#*").match(new URI("http://foobar.local:2345/somepath/more?par=val#frag")));
        assertTrue(new URIPattern("http://*.local/somepath/*?*#*").match(new URI("http://foobar.local/somepath/more")));
    }
}
