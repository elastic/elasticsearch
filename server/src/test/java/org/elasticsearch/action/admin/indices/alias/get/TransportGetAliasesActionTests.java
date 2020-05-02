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
package org.elasticsearch.action.admin.indices.alias.get;

import org.elasticsearch.cluster.metadata.AliasMetadata;
import org.elasticsearch.common.collect.ImmutableOpenMap;
import org.elasticsearch.test.ESTestCase;

import java.util.Collections;
import java.util.List;

import static org.hamcrest.Matchers.equalTo;

public class TransportGetAliasesActionTests extends ESTestCase {

    public void testPostProcess() {
        GetAliasesRequest request = new GetAliasesRequest();
        ImmutableOpenMap<String, List<AliasMetadata>> aliases = ImmutableOpenMap.<String, List<AliasMetadata>>builder()
            .fPut("b", Collections.singletonList(new AliasMetadata.Builder("y").build()))
            .build();
        ImmutableOpenMap<String, List<AliasMetadata>> result =
            TransportGetAliasesAction.postProcess(request, new String[]{"a", "b", "c"}, aliases);
        assertThat(result.size(), equalTo(3));
        assertThat(result.get("a").size(), equalTo(0));
        assertThat(result.get("b").size(), equalTo(1));
        assertThat(result.get("c").size(), equalTo(0));

        request = new GetAliasesRequest();
        request.replaceAliases("y", "z");
        aliases = ImmutableOpenMap.<String, List<AliasMetadata>>builder()
            .fPut("b", Collections.singletonList(new AliasMetadata.Builder("y").build()))
            .build();
        result = TransportGetAliasesAction.postProcess(request, new String[]{"a", "b", "c"}, aliases);
        assertThat(result.size(), equalTo(3));
        assertThat(result.get("a").size(), equalTo(0));
        assertThat(result.get("b").size(), equalTo(1));
        assertThat(result.get("c").size(), equalTo(0));

        request = new GetAliasesRequest("y", "z");
        aliases = ImmutableOpenMap.<String, List<AliasMetadata>>builder()
            .fPut("b", Collections.singletonList(new AliasMetadata.Builder("y").build()))
            .build();
        result = TransportGetAliasesAction.postProcess(request, new String[]{"a", "b", "c"}, aliases);
        assertThat(result.size(), equalTo(1));
        assertThat(result.get("b").size(), equalTo(1));
    }

}
