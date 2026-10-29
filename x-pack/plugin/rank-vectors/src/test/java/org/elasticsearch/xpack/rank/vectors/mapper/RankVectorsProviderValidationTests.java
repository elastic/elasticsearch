/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.rank.vectors.mapper;

import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.index.codec.CodecService;
import org.elasticsearch.index.codec.PerFieldMapperCodec;
import org.elasticsearch.index.mapper.MapperService;
import org.elasticsearch.index.mapper.MapperServiceTestCase;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.threadpool.TestThreadPool;
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
        MapperService mapperService = createMapperService(fieldMapping(b -> {
            b.field("type", "rank_vectors");
            b.field("dims", RankVectorsFieldMapper.MUVERA_TOKEN_DIMS);
            b.field("index", true);
            b.startObject("index_options");
            b.field("type", "muvera");
            b.field("encoding_version", RankVectorsFieldMapper.MUVERA_ENCODING_VERSION);
            b.field("encoding_profile", RankVectorsFieldMapper.MUVERA_ENCODING_PROFILE);
            b.endObject();
        }));
        try (TestThreadPool threadPool = new TestThreadPool(getTestName())) {
            PerFieldMapperCodec codec = (PerFieldMapperCodec) new CodecService(mapperService, BigArrays.NON_RECYCLING_INSTANCE, threadPool)
                .codec("default");
            IllegalStateException e = expectThrows(IllegalStateException.class, () -> codec.getKnnVectorsFormatForField("field"));
            assertThat(e.getMessage(), containsString("requires the [bbq_disk] vector format provider"));
        }
    }

    public void testNonIndexedRankVectorsDoesNotRequireDiskBbqProvider() throws Exception {
        createMapperService(fieldMapping(b -> b.field("type", "rank_vectors").field("dims", RankVectorsFieldMapper.MUVERA_TOKEN_DIMS)));
    }
}
