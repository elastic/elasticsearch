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

package org.elasticsearch.action.indexedscripts.get;

import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.index.VersionType;
import org.elasticsearch.test.ESTestCase;
import org.junit.Test;

import java.io.IOException;

import static org.elasticsearch.test.VersionUtils.randomVersion;
import static org.hamcrest.CoreMatchers.equalTo;

public class GetIndexedScriptRequestTests extends ESTestCase {

    @Test
    public void testGetIndexedScriptRequestSerialization() throws IOException {
        GetIndexedScriptRequest request = new GetIndexedScriptRequest("lang", "id");
        if (randomBoolean()) {
            request.version(randomIntBetween(1, Integer.MAX_VALUE));
            request.versionType(randomFrom(VersionType.values()));
        }

        BytesStreamOutput out = new BytesStreamOutput();
        out.setVersion(randomVersion(random()));
        request.writeTo(out);

        StreamInput in = StreamInput.wrap(out.bytes());
        in.setVersion(out.getVersion());
        GetIndexedScriptRequest request2 = new GetIndexedScriptRequest();
        request2.readFrom(in);

        assertThat(request2.id(), equalTo(request.id()));
        assertThat(request2.scriptLang(), equalTo(request.scriptLang()));
        assertThat(request2.version(), equalTo(request.version()));
        assertThat(request2.versionType(), equalTo(request.versionType()));
    }
}
