/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.action.search;

import org.elasticsearch.action.support.IndicesOptions;
import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.ArrayUtils;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.SearchModule;
import org.elasticsearch.test.AbstractWireSerializingTestCase;

import java.io.IOException;
import java.util.List;

public class SearchShardsRequestTests extends AbstractWireSerializingTestCase<SearchShardsRequest> {
    @Override
    protected NamedWriteableRegistry getNamedWriteableRegistry() {
        return new NamedWriteableRegistry(new SearchModule(Settings.EMPTY, List.of()).getNamedWriteables());
    }

    @Override
    protected Writeable.Reader<SearchShardsRequest> instanceReader() {
        return SearchShardsRequest::new;
    }

    @Override
    protected SearchShardsRequest createTestInstance() {
        String[] indices = generateRandomStringArray(10, 10, false);
        IndicesOptions indicesOptions = IndicesOptions.fromOptions(randomBoolean(), randomBoolean(), randomBoolean(), randomBoolean());
        QueryBuilder query = QueryBuilders.termQuery(randomAlphaOfLengthBetween(5, 20), randomAlphaOfLengthBetween(5, 20));
        String routing = randomBoolean() ? null : randomAlphaOfLength(10);
        String preference = randomBoolean() ? null : randomAlphaOfLength(10);
        String clusterAlias = randomBoolean() ? null : randomAlphaOfLength(10);
        return new SearchShardsRequest(indices, indicesOptions, query, routing, preference, randomBoolean(), clusterAlias);
    }

    @Override
    protected SearchShardsRequest mutateInstance(SearchShardsRequest r) throws IOException {
        return switch (between(0, 6)) {
            case 0 -> {
                String[] extraIndices = randomArray(1, 10, String[]::new, () -> randomAlphaOfLength(10));
                String[] indices = ArrayUtils.concat(r.indices(), extraIndices);
                yield new SearchShardsRequest(
                    indices,
                    r.indicesOptions(),
                    r.query(),
                    r.routing(),
                    r.preference(),
                    randomBoolean(),
                    r.clusterAlias()
                );
            }
            case 1 -> {
                IndicesOptions indicesOptions = randomValueOtherThan(
                    r.indicesOptions(),
                    () -> IndicesOptions.fromOptions(randomBoolean(), randomBoolean(), randomBoolean(), randomBoolean())
                );
                yield new SearchShardsRequest(
                    r.indices(),
                    indicesOptions,
                    r.query(),
                    r.routing(),
                    r.preference(),
                    randomBoolean(),
                    r.clusterAlias()
                );
            }
            case 2 -> {
                QueryBuilder query = QueryBuilders.rangeQuery(randomAlphaOfLengthBetween(5, 20)).from(randomNonNegativeLong());
                yield new SearchShardsRequest(
                    r.indices(),
                    r.indicesOptions(),
                    query,
                    r.routing(),
                    r.preference(),
                    randomBoolean(),
                    r.clusterAlias()
                );
            }
            case 3 -> {
                String routing = randomValueOtherThan(r.routing(), () -> randomBoolean() ? null : randomAlphaOfLength(10));
                yield new SearchShardsRequest(
                    r.indices(),
                    r.indicesOptions(),
                    r.query(),
                    routing,
                    r.preference(),
                    randomBoolean(),
                    r.clusterAlias()
                );
            }
            case 4 -> {
                String preference = randomValueOtherThan(r.preference(), () -> randomBoolean() ? null : randomAlphaOfLength(10));
                yield new SearchShardsRequest(
                    r.indices(),
                    r.indicesOptions(),
                    r.query(),
                    r.routing(),
                    preference,
                    randomBoolean(),
                    r.clusterAlias()
                );
            }
            case 5 -> new SearchShardsRequest(
                r.indices(),
                r.indicesOptions(),
                r.query(),
                r.routing(),
                r.preference(),
                r.allowPartialSearchResults() == false,
                r.clusterAlias()
            );
            case 6 -> {
                String clusterAlias = randomValueOtherThan(r.clusterAlias(), () -> randomBoolean() ? null : randomAlphaOfLength(10));
                yield new SearchShardsRequest(
                    r.indices(),
                    r.indicesOptions(),
                    r.query(),
                    r.routing(),
                    r.preference(),
                    randomBoolean(),
                    clusterAlias
                );
            }
            default -> throw new AssertionError("unexpected value");
        };
    }
}
