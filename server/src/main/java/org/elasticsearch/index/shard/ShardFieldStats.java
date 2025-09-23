/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.shard;

import org.apache.lucene.util.FixedBitSet;
import org.apache.lucene.util.RamUsageEstimator;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.xcontent.ConstructingObjectParser;
import org.elasticsearch.xcontent.ParseField;
import org.elasticsearch.xcontent.ToXContent;
import org.elasticsearch.xcontent.ToXContentFragment;
import org.elasticsearch.xcontent.XContentBuilder;

import java.io.IOException;

import static org.elasticsearch.xcontent.ConstructingObjectParser.constructorArg;

/**
 * A per shard stats including the number of segments and total fields across those segments.
 * These stats should be recomputed whenever the shard is refreshed.
 *
 * @param numSegments           the number of segments
 * @param totalFields           the total number of fields across the segments
 * @param fieldUsages           the number of usages for segment-level fields (e.g., doc_values, postings, norms, points)
 *                              -1 if unavailable
 * @param postingsInMemoryBytes the total bytes in memory used for postings across all fields
 * @param liveDocsBytes         the total bytes in memory used for live docs
 */
public record ShardFieldStats(int numSegments, int totalFields, long fieldUsages, long postingsInMemoryBytes, long liveDocsBytes)
    implements
        ToXContentFragment {

    public static final long FIXED_BITSET_BASE_RAM_BYTES_USED = RamUsageEstimator.shallowSizeOfInstance(FixedBitSet.class);

    static final class Fields {
        static final String SHARD_FIELD_STATS = "shard_field_stats";
        static final String NUM_SEGMENTS = "num_segments";
        static final String TOTAL_FIELDS = "total_fields";
        static final String FIELD_USAGES = "field_usages";
        static final String POSTINGS_IN_MEMORY_BYTES = "postings_in_memory_bytes";
        static final String POSTINGS_IN_MEMORY = "postings_in_memory";
        static final String LIVE_DOCS_BYTES = "live_docs_bytes";
        static final String LIVE_DOCS = "live_docs";
    }

    public static final ConstructingObjectParser<ShardFieldStats, Void> PARSER = new ConstructingObjectParser<>(
        "shard_field_stats",
        true,
        args -> (ShardFieldStats) args[0]
    );

    protected static final ConstructingObjectParser<ShardFieldStats, Void> SHARD_FIELD_STATS_PARSER = new ConstructingObjectParser<>(
        "shard_field_stats_fields",
        true,
        args -> new ShardFieldStats((int) args[0], (int) args[1], (long) args[2], (long) args[3], (long) args[4])
    );

    static {
        PARSER.declareObject(constructorArg(), SHARD_FIELD_STATS_PARSER, new ParseField(Fields.SHARD_FIELD_STATS));
    }

    static {
        SHARD_FIELD_STATS_PARSER.declareInt(constructorArg(), new ParseField(Fields.NUM_SEGMENTS));
        SHARD_FIELD_STATS_PARSER.declareInt(constructorArg(), new ParseField(Fields.TOTAL_FIELDS));
        SHARD_FIELD_STATS_PARSER.declareLong(constructorArg(), new ParseField(Fields.FIELD_USAGES));
        SHARD_FIELD_STATS_PARSER.declareLong(constructorArg(), new ParseField(Fields.POSTINGS_IN_MEMORY_BYTES));
        SHARD_FIELD_STATS_PARSER.declareLong(constructorArg(), new ParseField(Fields.LIVE_DOCS_BYTES));
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, ToXContent.Params params) throws IOException {
        builder.startObject(Fields.SHARD_FIELD_STATS);
        builder.field(Fields.NUM_SEGMENTS, numSegments);
        builder.field(Fields.TOTAL_FIELDS, totalFields);
        builder.field(Fields.FIELD_USAGES, fieldUsages);
        builder.humanReadableField(
            Fields.POSTINGS_IN_MEMORY_BYTES,
            Fields.POSTINGS_IN_MEMORY,
            ByteSizeValue.ofBytes(postingsInMemoryBytes)
        );
        builder.humanReadableField(Fields.LIVE_DOCS_BYTES, Fields.LIVE_DOCS, ByteSizeValue.ofBytes(liveDocsBytes));
        builder.endObject();
        return builder;
    }

}
