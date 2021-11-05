/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.index.rankeval;

import org.elasticsearch.action.search.SearchType;
import org.elasticsearch.action.support.IndicesOptions;
import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.io.stream.Writeable.Reader;
import org.elasticsearch.common.util.ArrayUtils;
import org.elasticsearch.test.AbstractWireSerializingTestCase;
import org.elasticsearch.xcontent.NamedXContentRegistry;
import org.junit.AfterClass;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class RankEvalRequestTests extends AbstractWireSerializingTestCase<RankEvalRequest> {

    private static RankEvalPlugin rankEvalPlugin = new RankEvalPlugin();

    @AfterClass
    public static void releasePluginResources() throws IOException {
        rankEvalPlugin.close();
    }

    @Override
    protected NamedXContentRegistry xContentRegistry() {
        return new NamedXContentRegistry(rankEvalPlugin.getNamedXContent());
    }

    @Override
    protected NamedWriteableRegistry getNamedWriteableRegistry() {
        return new NamedWriteableRegistry(rankEvalPlugin.getNamedWriteables());
    }

    @Override
    protected RankEvalRequest createTestInstance() {
        int numberOfIndices = randomInt(3);
        String[] indices = new String[numberOfIndices];
        for (int i = 0; i < numberOfIndices; i++) {
            indices[i] = randomAlphaOfLengthBetween(5, 10);
        }
        RankEvalRequest rankEvalRequest = new RankEvalRequest(RankEvalSpecTests.createTestItem(), indices);
        IndicesOptions indicesOptions = IndicesOptions.fromOptions(
            randomBoolean(),
            randomBoolean(),
            randomBoolean(),
            randomBoolean(),
            randomBoolean(),
            randomBoolean(),
            randomBoolean(),
            randomBoolean()
        );
        rankEvalRequest.indicesOptions(indicesOptions);
        rankEvalRequest.searchType(randomFrom(SearchType.DFS_QUERY_THEN_FETCH, SearchType.QUERY_THEN_FETCH));
        return rankEvalRequest;
    }

    @Override
    protected Reader<RankEvalRequest> instanceReader() {
        return RankEvalRequest::new;
    }

    @Override
    protected RankEvalRequest mutateInstance(RankEvalRequest instance) throws IOException {
        RankEvalRequest mutation = copyInstance(instance);
        List<Runnable> mutators = new ArrayList<>();
        mutators.add(() -> mutation.indices(ArrayUtils.concat(instance.indices(), new String[] { randomAlphaOfLength(10) })));
        mutators.add(
            () -> mutation.indicesOptions(
                randomValueOtherThan(
                    instance.indicesOptions(),
                    () -> IndicesOptions.fromOptions(randomBoolean(), randomBoolean(), randomBoolean(), randomBoolean())
                )
            )
        );
        mutators.add(() -> {
            if (instance.searchType() == SearchType.DFS_QUERY_THEN_FETCH) {
                mutation.searchType(SearchType.QUERY_THEN_FETCH);
            } else {
                mutation.searchType(SearchType.DFS_QUERY_THEN_FETCH);
            }
        });
        mutators.add(() -> mutation.setRankEvalSpec(RankEvalSpecTests.mutateTestItem(instance.getRankEvalSpec())));
        mutators.add(() -> mutation.setRankEvalSpec(RankEvalSpecTests.mutateTestItem(instance.getRankEvalSpec())));
        randomFrom(mutators).run();
        return mutation;
    }

}
