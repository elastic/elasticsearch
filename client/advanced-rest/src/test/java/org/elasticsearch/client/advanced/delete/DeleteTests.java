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

package org.elasticsearch.client.advanced.delete;

import org.elasticsearch.client.advanced.RequestTestUtil;
import org.elasticsearch.test.ESTestCase;

public class DeleteTests extends ESTestCase {

    public void testDelete() {
        RequestTestUtil.assertThrows(DeleteRestRequest.builder().build(),
            IllegalArgumentException.class,
            "Index can not be null");
        RequestTestUtil.assertThrows(DeleteRestRequest.builder().setIndex("foo").build(),
            IllegalArgumentException.class,
            "Type can not be null");
        RequestTestUtil.assertThrows(DeleteRestRequest.builder().setIndex("foo").setType("bar").build(),
            IllegalArgumentException.class,
            "Id can not be null");
        RequestTestUtil.assertNoException(DeleteRestRequest.builder().setIndex("foo").setType("bar").setId("id").build());
    }

}
