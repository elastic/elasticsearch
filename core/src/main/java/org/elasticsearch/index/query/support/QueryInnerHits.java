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
package org.elasticsearch.index.query.support;

import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.action.support.ToXContentToBytes;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.search.fetch.innerhits.InnerHitsBuilder;

import java.io.IOException;

/**
 */
public class QueryInnerHits extends ToXContentToBytes implements Writeable<QueryInnerHits> {
    private final BytesReference queryInnerHitsSearchSource;

    public QueryInnerHits(StreamInput input) throws IOException {
        queryInnerHitsSearchSource = input.readBytesReference();
    }

    public QueryInnerHits(XContentParser parser) throws IOException {
        BytesStreamOutput out = new BytesStreamOutput();
        try (XContentBuilder builder = XContentFactory.cborBuilder(out)) {
            builder.copyCurrentStructure(parser);
            queryInnerHitsSearchSource = builder.bytes();
        }
    }

    public QueryInnerHits() {
        this(null, null);
    }

    public QueryInnerHits(String name, InnerHitsBuilder.InnerHit innerHit) {
        BytesStreamOutput out = new BytesStreamOutput();
        try (XContentBuilder builder = XContentFactory.cborBuilder(out)) {
            builder.startObject();
            if (name != null) {
                builder.field("name", name);
            }
            if (innerHit != null) {
                innerHit.toXContent(builder, ToXContent.EMPTY_PARAMS);
            }
            builder.endObject();
            this.queryInnerHitsSearchSource = builder.bytes();
        } catch (IOException e) {
            throw new ElasticsearchException("failed to build xcontent", e);
        }
    }

    @Override
    public QueryInnerHits readFrom(StreamInput in) throws IOException {
        return new QueryInnerHits(in);
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.field("inner_hits");
        try (XContentParser parser = XContentType.CBOR.xContent().createParser(queryInnerHitsSearchSource)) {
            builder.copyCurrentStructure(parser);
        }
        return builder;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeBytesReference(queryInnerHitsSearchSource);
    }

    public XContentParser getXcontentParser() throws IOException {
        return XContentType.CBOR.xContent().createParser(queryInnerHitsSearchSource);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        QueryInnerHits that = (QueryInnerHits) o;

        return queryInnerHitsSearchSource.equals(that.queryInnerHitsSearchSource);

    }

    @Override
    public int hashCode() {
        return queryInnerHitsSearchSource.hashCode();
    }
}
