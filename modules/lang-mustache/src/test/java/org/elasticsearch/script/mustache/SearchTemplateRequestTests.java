/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
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

        mutators.add(
            request -> request.setScriptType(randomValueOtherThan(request.getScriptType(), () -> randomFrom(ScriptType.values())))
        );
        mutators.add(request -> request.setScript(randomValueOtherThan(request.getScript(), () -> randomAlphaOfLength(50))));

        mutators.add(request -> {
            Map<String, Object> mutatedScriptParams = new HashMap<>(request.getScriptParams());
            String newField = randomValueOtherThanMany(mutatedScriptParams::containsKey, () -> randomAlphaOfLength(5));
            mutatedScriptParams.put(newField, randomAlphaOfLength(10));
            request.setScriptParams(mutatedScriptParams);
        });

        mutators.add(request -> request.setProfile(request.isProfile() == false));
        mutators.add(request -> request.setExplain(request.isExplain() == false));
        mutators.add(request -> request.setSimulate(request.isSimulate() == false));

        mutators.add(
            request -> request.setRequest(
                randomValueOtherThan(
                    request.getRequest(),
                    () -> RandomSearchRequestGenerator.randomSearchRequest(SearchSourceBuilder::searchSource)
                )
            )
        );

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

        request.setRequest(RandomSearchRequestGenerator.randomSearchRequest(SearchSourceBuilder::searchSource));
        return request;
    }
}
