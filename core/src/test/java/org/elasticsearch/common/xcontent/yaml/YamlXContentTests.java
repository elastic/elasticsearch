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

package org.elasticsearch.common.xcontent.yaml;

import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;

import org.elasticsearch.common.xcontent.BaseXContentTestCase;
import org.elasticsearch.common.xcontent.XContentType;

import java.io.ByteArrayOutputStream;

public class YamlXContentTests extends BaseXContentTestCase {

    @Override
    public XContentType xcontentType() {
        return XContentType.YAML;
    }

    public void testBigInteger() throws Exception {
        ByteArrayOutputStream os = new ByteArrayOutputStream();
        JsonGenerator generator = new YAMLFactory().createGenerator(os);
        doTestBigInteger(generator, os);
    }
}
