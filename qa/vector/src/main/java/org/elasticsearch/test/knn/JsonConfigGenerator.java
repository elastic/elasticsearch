/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.test.knn;


import org.elasticsearch.common.Strings;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.json.JsonXContent;

import java.io.IOException;

public class JsonConfigGenerator {
    public static void main(String[] args) throws IOException {
        // Create the base JSON structure
        XContentBuilder builder = JsonXContent.contentBuilder().prettyPrint().humanReadable(true);
        builder.startArray(); // Start the array

        for (int quantizeBits = 1; quantizeBits <= 7; quantizeBits++) {
            boolean reindex = true;
            for (int quantizeQueryBits = quantizeBits; quantizeQueryBits <= 7; quantizeQueryBits++) {
                for (int overSamplingFactor = 1; overSamplingFactor <= 5; overSamplingFactor++) {
                    builder.startObject(); // Start a new JSON object
                    builder.field("doc_vectors", "/Users/cdelgado/dev/workspace/ann-prototypes/data/corpus-ann-gist-1M.fvec");
                    builder.field("query_vectors", "/Users/cdelgado/dev/workspace/ann-prototypes/data/queries-ann-gist-1M.fvec");
                    builder.field("num_docs", 1000000);
                    builder.field("num_queries", 100);
                    builder.field("index_type", "flat");
                    builder.field("search_threads", 20);
                    builder.field("index_threads", 20);
                    builder.field("reindex", reindex);
                    builder.field("force_merge", true);
                    builder.field("vector_space", "euclidean");
                    builder.field("vector_encoding", "float32");
                    builder.field("dimensions", -1);
                    builder.field("use_new_flat_vectors_format", true);
                    builder.field("quantize_bits", quantizeBits);
                    builder.field("quantize_query_bits", quantizeQueryBits);
                    builder.field("over_sampling_factor", overSamplingFactor);
                    builder.endObject(); // End the JSON object
                    reindex = false;
                }
            }
        }

        builder.endArray(); // End the array

        // Print the generated JSON
        System.out.println(Strings.toString(builder));
    }
}
