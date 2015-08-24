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

package org.elasticsearch.common.settings.loader;

import org.elasticsearch.ElasticsearchParseException;
import org.elasticsearch.test.ESTestCase;

import java.io.IOException;
import java.nio.charset.Charset;

public class PropertiesSettingsLoaderTests extends ESTestCase {
    public void testDuplicateKeyFromStringThrowsException() throws IOException {
        PropertiesSettingsLoader loader = new PropertiesSettingsLoader();
        try {
            loader.load("foo=bar\nfoo=baz");
            fail("expected exception");
        } catch (ElasticsearchParseException e) {
            assertEquals(e.getMessage(), "duplicate settings key [foo] found, previous value [bar], current value [baz]");
        }
    }

    public void testDuplicateKeysFromBytesThrowsException() throws IOException {
        PropertiesSettingsLoader loader = new PropertiesSettingsLoader();
        try {
            loader.load("foo=bar\nfoo=baz".getBytes(Charset.defaultCharset()));
        } catch (ElasticsearchParseException e) {
            assertEquals(e.getMessage(), "duplicate settings key [foo] found, previous value [bar], current value [baz]");
        }
    }
}
