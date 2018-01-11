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
package org.elasticsearch.common;

import org.elasticsearch.test.ESTestCase;

/**
 * Tests that the {@link LoggingDeprecationHandler} produces deprecation warnings
 * in the expected way. Tools are built around this way so be weary of changing
 * the patterns in this test.
 */
public class LoggingDeprecationHandlerTests extends ESTestCase {
    public void testUsedDeprecatedName() {
        LoggingDeprecationHandler.INSTANCE.usedDeprecatedName("bar_foo", "foo_bar");
        assertWarnings("Deprecated field [bar_foo] used, expected [foo_bar] instead");
    }

    public void testUsedDeprecatedField() {
        LoggingDeprecationHandler.INSTANCE.usedDeprecatedField("like_text", "like");
        assertWarnings("Deprecated field [like_text] used, replaced by [like]");
    }
}
