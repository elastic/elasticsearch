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
package org.elasticsearch.index.mapper;

import java.io.IOException;
import java.util.Set;

import static org.hamcrest.Matchers.hasItem;

public abstract class AbstractNumericFieldMapperTestCase extends MapperTestCase {
    protected abstract Set<String> types();
    protected abstract Set<String> wholeTypes();

    public final void testTypesAndWholeTypes() {
        for (String wholeType : wholeTypes()) {
            assertThat(types(), hasItem(wholeType));
        }
    }

    public final void testDefaults() throws Exception {
        for (String type : types()) {
            doTestDefaults(type);
        }
    }

    protected abstract void doTestDefaults(String type) throws Exception;

    public final void testNotIndexed() throws Exception {
        for (String type : types()) {
            doTestNotIndexed(type);
        }
    }

    protected abstract void doTestNotIndexed(String type) throws Exception;

    public final void testNoDocValues() throws Exception {
        for (String type : types()) {
            doTestNoDocValues(type);
        }
    }

    protected abstract void doTestNoDocValues(String type) throws Exception;

    public final void testStore() throws Exception {
        for (String type : types()) {
            doTestStore(type);
        }
    }

    protected abstract void doTestStore(String type) throws Exception;

    public final void testCoerce() throws Exception {
        for (String type : types()) {
            doTestCoerce(type);
        }
    }

    protected abstract void doTestCoerce(String type) throws IOException;

    public final void testDecimalCoerce() throws Exception {
        for (String type : wholeTypes()) {
            doTestDecimalCoerce(type);
        }
    }

    protected abstract void doTestDecimalCoerce(String type) throws IOException;

    public final void testNullValue() throws IOException {
        for (String type : types()) {
            doTestNullValue(type);
        }
    }

    protected abstract void doTestNullValue(String type) throws IOException;
}
