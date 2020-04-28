/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.elasticsearch.action.admin.indices.create;

import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.test.AbstractWireSerializingTestCase;

import java.util.NavigableMap;
import java.util.TreeMap;

public class AutoCreateResponseTests extends AbstractWireSerializingTestCase<AutoCreateAction.Response> {

    @Override
    protected Writeable.Reader<AutoCreateAction.Response> instanceReader() {
        return AutoCreateAction.Response::new;
    }

    @Override
    protected AutoCreateAction.Response createTestInstance() {
        int numEntries = randomIntBetween(0, 16);
        NavigableMap<String, Exception> map = new TreeMap<>();
        for (int i = 0; i < numEntries; i++) {
            map.put(randomAlphaOfLength(4), new ElasticsearchException(randomAlphaOfLength(4)));
        }
        return new AutoCreateAction.Response(map);
    }
}
