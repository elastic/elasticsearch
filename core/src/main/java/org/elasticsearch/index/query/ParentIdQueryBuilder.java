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
import org.apache.lucene.search.BooleanClause;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.DocValuesTermsQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.TermQuery;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.index.mapper.DocumentMapper;
import org.elasticsearch.index.mapper.internal.ParentFieldMapper;
import org.elasticsearch.index.mapper.internal.TypeFieldMapper;

import java.io.IOException;
import java.util.Objects;

public final class ParentIdQueryBuilder extends AbstractQueryBuilder<ParentIdQueryBuilder> {

    public static final String NAME = "parent_id";
    static final ParentIdQueryBuilder PROTO = new ParentIdQueryBuilder(null, null);

    private final String type;
    private final String id;

    public ParentIdQueryBuilder(String type, String id) {
        this.type = type;
        this.id = id;
    }

    public String getType() {
        return type;
    }

    public String getId() {
        return id;
    }

    @Override
    protected void doXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject(NAME);
        builder.field(ParentIdQueryParser.TYPE_FIELD.getPreferredName(), type);
        builder.field(ParentIdQueryParser.ID_FIELD.getPreferredName(), id);
        printBoostAndQueryName(builder);
        builder.endObject();
    }

    @Override
    protected Query doToQuery(QueryShardContext context) throws IOException {
        DocumentMapper childDocMapper = context.getMapperService().documentMapper(type);
        if (childDocMapper == null) {
            throw new QueryShardException(context, "[" + NAME + "] no mapping found for type [" + type + "]");
        }
        ParentFieldMapper parentFieldMapper = childDocMapper.parentFieldMapper();
        if (parentFieldMapper.active() == false) {
            throw new QueryShardException(context, "[" + NAME + "] _parent field has no parent type configured");
        }
        String fieldName = ParentFieldMapper.joinField(parentFieldMapper.type());

        BooleanQuery.Builder query = new BooleanQuery.Builder();
        query.add(new DocValuesTermsQuery(fieldName, id), BooleanClause.Occur.MUST);
        // Need to take child type into account, otherwise a child doc of different type with the same id could match
        query.add(new TermQuery(new Term(TypeFieldMapper.NAME, type)), BooleanClause.Occur.FILTER);
        return query.build();
    }

    @Override
    protected ParentIdQueryBuilder doReadFrom(StreamInput in) throws IOException {
        String type = in.readString();
        String id = in.readString();
        return new ParentIdQueryBuilder(type, id);
    }

    @Override
    protected void doWriteTo(StreamOutput out) throws IOException {
        out.writeString(type);
        out.writeString(id);
    }

    @Override
    protected boolean doEquals(ParentIdQueryBuilder that) {
        return Objects.equals(type, that.type) && Objects.equals(id, that.id);
    }

    @Override
    protected int doHashCode() {
        return Objects.hash(type, id);
    }

    @Override
    public String getWriteableName() {
        return NAME;
    }
}
