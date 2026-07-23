/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.action.bulk;

import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.IndexVersion;
import org.elasticsearch.index.mapper.LuceneDocument;
import org.elasticsearch.index.mapper.ShardBatchMapper;
import org.elasticsearch.index.shard.IndexShard;
import org.elasticsearch.index.shard.IndexShardTestCase;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentFactory;

import java.io.IOException;

/**
 * Parse-time tests for the batch-mapping fast path: drives {@link ShardBatchMapper} directly and
 * verifies the resulting {@link LuceneDocument} fields carry the expected values. Engine indexing
 * is intentionally not exercised here — those interactions are covered by {@code ShardBatchIndexer}
 * tests; this file's job is to lock down the mapper's per-row parsing contract.
 */
public class ShardBatchMapperParseTests extends IndexShardTestCase {

    private static final Settings SYNTHETIC_SOURCE_SETTINGS = indexSettings(IndexVersion.current(), 1, 0).put(
        "index.mapping.source.mode",
        "synthetic"
    ).build();

    private static final Settings STORED_SOURCE_SETTINGS = indexSettings(IndexVersion.current(), 1, 0).build();

    private IndexShard newShardWithMapping(String mapping) throws IOException {
        return newShardWithMapping(mapping, SYNTHETIC_SOURCE_SETTINGS);
    }

    private IndexShard newShardWithMapping(String mapping, Settings settings) throws IOException {
        IndexMetadata md = IndexMetadata.builder("index").putMapping(mapping).settings(settings).primaryTerm(0, 1).build();
        IndexShard shard = newShard(new ShardId(md.getIndex(), 0), true, "n1", md, null);
        recoverShardFromStore(shard);
        return shard;
    }

    private static IndexRequest indexRequest(String id) {
        return new IndexRequest("index").id(id);
    }

    /** Builds a single-document JSON {@link BytesReference} from alternating name/value pairs. */
    private static BytesReference doc(Object... kvPairs) throws IOException {
        try (XContentBuilder b = XContentFactory.jsonBuilder()) {
            b.startObject();
            for (int i = 0; i < kvPairs.length; i += 2) {
                b.field((String) kvPairs[i], kvPairs[i + 1]);
            }
            b.endObject();
            return BytesReference.bytes(b);
        }
    }

    // TODO(columnar): the tests below drove the row-major ShardBatchMapper.parseMappings, which was
    // replaced by the columnar batch-mapping path (mapColumnBatch). Re-enable (adapted to the
    // columnar API) once field (non-metadata) mappers support columnar parsing:
    // - testSupportedMapperTypes (date, keyword, long, double)
    // - testResolveFallsBackForUnsupportedMapper (text+index_prefixes triggers sequential fallback)
    // - testIgnoredLeafUnderDynamicFalseParentIsDropped
    // - testNumberMapperReceivesStringValue
    // - testIgnoreAboveOnKeywordDoesNotFail
    // - testParseMappingsAddsMetadataFields (id, routing, version, seq_no, source via preParse/postParse)
    // - testParseMappingsSyntheticSourceAndIgnored
    // - testNullValuesAreSkipped
    // - testBooleanMapper
    // - testIpMapper
    // - testIpMapperIgnoreMalformed
    // - testTextMapper

}
