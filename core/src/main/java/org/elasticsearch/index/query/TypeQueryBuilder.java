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

package org.elasticsearch.index.query;

import org.apache.lucene.index.Term;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.lucene.BytesRefs;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.index.mapper.DocumentMapper;
import org.elasticsearch.index.mapper.internal.TypeFieldMapper;

import java.io.IOException;
import java.util.Objects;

public class TypeQueryBuilder extends AbstractQueryBuilder<TypeQueryBuilder> {

    public static final String NAME = "type";

    private final BytesRef type;

    static final TypeQueryBuilder PROTOTYPE = new TypeQueryBuilder("type");

    public TypeQueryBuilder(String type) {
        if (type == null) {
            throw new IllegalArgumentException("[type] cannot be null");
        }
        this.type = BytesRefs.toBytesRef(type);
    }

    TypeQueryBuilder(BytesRef type) {
        if (type == null) {
            throw new IllegalArgumentException("[type] cannot be null");
        }
        this.type = type;
    }

    public String type() {
        return BytesRefs.toString(this.type);
    }

    @Override
    protected void doXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject(NAME);
        builder.field(TypeQueryParser.VALUE_FIELD.getPreferredName(), type.utf8ToString());
        printBoostAndQueryName(builder);
        builder.endObject();
    }

    @Override
    public String getWriteableName() {
        return NAME;
    }

    @Override
    protected Query doToQuery(QueryShardContext context) throws IOException {
        Query filter;
        //LUCENE 4 UPGRADE document mapper should use bytesref as well?
        DocumentMapper documentMapper = context.getMapperService().documentMapper(type.utf8ToString());
        if (documentMapper == null) {
            filter = new TermQuery(new Term(TypeFieldMapper.NAME, type));
        } else {
            filter = documentMapper.typeFilter();
        }
        return filter;
    }

    @Override
    protected TypeQueryBuilder doReadFrom(StreamInput in) throws IOException {
        return new TypeQueryBuilder(in.readBytesRef());
    }

    @Override
    protected void doWriteTo(StreamOutput out) throws IOException {
        out.writeBytesRef(type);
    }

    @Override
    protected int doHashCode() {
        return Objects.hash(type);
    }

    @Override
    protected boolean doEquals(TypeQueryBuilder other) {
        return Objects.equals(type, other.type);
    }
}