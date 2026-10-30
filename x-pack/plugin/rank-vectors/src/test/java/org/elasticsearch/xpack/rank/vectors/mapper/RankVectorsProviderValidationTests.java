/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.rank.vectors.mapper;

import org.elasticsearch.index.mapper.MapperParsingException;
import org.elasticsearch.index.mapper.MapperServiceTestCase;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.xpack.rank.vectors.LocalStateRankVectors;

import java.util.Collection;
import java.util.List;

import static org.hamcrest.Matchers.containsString;

public class RankVectorsProviderValidationTests extends MapperServiceTestCase {

    @Override
    protected Collection<? extends Plugin> getPlugins() {
        return List.of(new LocalStateRankVectors(SETTINGS));
    }

    public void testIndexedMuveraRequiresDiskBbqProvider() throws Exception {
        MapperParsingException e = expectThrows(MapperParsingException.class, () -> createMapperService(fieldMapping(b -> {
            b.field("type", "rank_vectors");
            b.field("dims", RankVectorsFieldMapper.MUVERA_TOKEN_DIMS);
            b.field("index", true);
        })));
        assertThat(e.getMessage(), containsString("require the [bbq_disk] vector format provider"));
    }

    public void testNonIndexedRankVectorsDoesNotRequireDiskBbqProvider() throws Exception {
        createMapperService(fieldMapping(b -> b.field("type", "rank_vectors").field("dims", RankVectorsFieldMapper.MUVERA_TOKEN_DIMS)));
    }
}
