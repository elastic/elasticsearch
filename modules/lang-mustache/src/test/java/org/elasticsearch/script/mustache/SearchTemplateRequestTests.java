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

package org.elasticsearch.script.mustache;

import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.script.ScriptType;
import org.elasticsearch.search.RandomSearchRequestGenerator;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.test.AbstractWireSerializingTestCase;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Consumer;

public class SearchTemplateRequestTests extends AbstractWireSerializingTestCase<SearchTemplateRequest> {

    @Override
    protected SearchTemplateRequest createTestInstance() {
        return createRandomRequest();
    }

    @Override
    protected Writeable.Reader<SearchTemplateRequest> instanceReader() {
        return SearchTemplateRequest::new;
    }

    @Override
    protected SearchTemplateRequest mutateInstance(SearchTemplateRequest instance) throws IOException {
        List<Consumer<SearchTemplateRequest>> mutators = new ArrayList<>();

        mutators.add(request -> request.setScriptType(
            randomValueOtherThan(request.getScriptType(), () -> randomFrom(ScriptType.values()))));
        mutators.add(request -> request.setScript(
            randomValueOtherThan(request.getScript(), () -> randomAlphaOfLength(50))));

        mutators.add(request -> {
            Map<String, Object> mutatedScriptParams = new HashMap<>(request.getScriptParams());
            String newField = randomValueOtherThanMany(mutatedScriptParams::containsKey, () -> randomAlphaOfLength(5));
            mutatedScriptParams.put(newField, randomAlphaOfLength(10));
            request.setScriptParams(mutatedScriptParams);
        });

        mutators.add(request -> request.setProfile(!request.isProfile()));
        mutators.add(request -> request.setExplain(!request.isExplain()));
        mutators.add(request -> request.setSimulate(!request.isSimulate()));

        mutators.add(request -> request.setRequest(randomValueOtherThan(request.getRequest(),
                () -> RandomSearchRequestGenerator.randomSearchRequest(SearchSourceBuilder::searchSource))));

        SearchTemplateRequest mutatedInstance = copyInstance(instance);
        Consumer<SearchTemplateRequest> mutator = randomFrom(mutators);
        mutator.accept(mutatedInstance);
        return mutatedInstance;
    }


    public static SearchTemplateRequest createRandomRequest() {
        SearchTemplateRequest request = new SearchTemplateRequest();
        request.setScriptType(randomFrom(ScriptType.values()));
        request.setScript(randomAlphaOfLength(50));

        Map<String, Object> scriptParams = new HashMap<>();
        for (int i = 0; i < randomInt(10); i++) {
            scriptParams.put(randomAlphaOfLength(5), randomAlphaOfLength(10));
        }
        request.setScriptParams(scriptParams);

        request.setExplain(randomBoolean());
        request.setProfile(randomBoolean());
        request.setSimulate(randomBoolean());

        request.setRequest(RandomSearchRequestGenerator.randomSearchRequest(
            SearchSourceBuilder::searchSource));
        return request;
    }
}
