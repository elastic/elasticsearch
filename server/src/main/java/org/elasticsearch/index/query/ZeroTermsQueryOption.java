/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.query;

import org.apache.lucene.search.Query;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.lucene.search.Queries;

import java.io.IOException;

public enum ZeroTermsQueryOption implements Writeable {
    NONE(0) {
        public Query asQuery() {
            return Queries.newMatchNoDocsQuery("Matching no documents because no terms present");
        }
    },
    ALL(1) {
        public Query asQuery() {
            return Queries.newMatchAllQuery();
        }
    },
    // this is used internally to make sure that query_string and simple_query_string
    // ignores query part that removes all tokens.
    NULL(2) {
        public Query asQuery() {
            return null;
        }
    };

    private final int ordinal;

    ZeroTermsQueryOption(int ordinal) {
        this.ordinal = ordinal;
    }

    public abstract Query asQuery();

    public static ZeroTermsQueryOption readFromStream(StreamInput in) throws IOException {
        int ord = in.readVInt();
        for (ZeroTermsQueryOption zeroTermsQuery : ZeroTermsQueryOption.values()) {
            if (zeroTermsQuery.ordinal == ord) {
                return zeroTermsQuery;
            }
        }
        throw new ElasticsearchException("unknown serialized type [" + ord + "]");
    }

    public static ZeroTermsQueryOption readFromString(String input) {
        for (ZeroTermsQueryOption zeroTermsQuery : ZeroTermsQueryOption.values()) {
            if (zeroTermsQuery.name().equalsIgnoreCase(input)) {
                return zeroTermsQuery;
            }
        }
        throw new ElasticsearchException("unknown serialized type [" + input + "]");
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeVInt(this.ordinal);
    }
}
