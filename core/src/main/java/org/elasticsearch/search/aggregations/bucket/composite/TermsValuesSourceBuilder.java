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

package org.elasticsearch.search.aggregations.bucket.composite;

import org.apache.lucene.search.SortField;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.xcontent.ObjectParser;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.search.aggregations.support.FieldContext;
import org.elasticsearch.search.aggregations.support.ValuesSource;
import org.elasticsearch.search.aggregations.support.ValuesSourceConfig;
import org.elasticsearch.search.internal.SearchContext;
import org.elasticsearch.script.Script;
import org.elasticsearch.search.sort.SortOrder;

import java.io.IOException;

/**
 * A {@link CompositeValuesSourceBuilder} that builds a {@link ValuesSource} from a {@link Script} or
 * a field name.
 */
public class TermsValuesSourceBuilder extends CompositeValuesSourceBuilder<TermsValuesSourceBuilder> {
    static final String TYPE = "terms";

    private static final ObjectParser<TermsValuesSourceBuilder, Void> PARSER;
    static {
        PARSER = new ObjectParser<>(TermsValuesSourceBuilder.TYPE);
        CompositeValuesSourceParserHelper.declareValuesSourceFields(PARSER, null);
    }
    static TermsValuesSourceBuilder parse(String name, XContentParser parser) throws IOException {
        return PARSER.parse(parser, new TermsValuesSourceBuilder(name), null);
    }

    public TermsValuesSourceBuilder(String name) {
        super(name);
    }

    protected TermsValuesSourceBuilder(StreamInput in) throws IOException {
        super(in);
    }

    @Override
    protected void innerWriteTo(StreamOutput out) throws IOException {}

    @Override
    protected void doXContentBody(XContentBuilder builder, Params params) throws IOException {}

    @Override
    protected int innerHashCode() {
        return 0;
    }

    @Override
    protected boolean innerEquals(TermsValuesSourceBuilder builder) {
        return true;
    }

    @Override
    public String type() {
        return TYPE;
    }

    @Override
    protected CompositeValuesSourceConfig innerBuild(SearchContext context,
                                                     ValuesSourceConfig<?> config,
                                                     int pos,
                                                     int numPos,
                                                     SortField sortField) throws IOException {
        ValuesSource vs = config.toValuesSource(context.getQueryShardContext());
        if (vs == null) {
            vs = ValuesSource.Numeric.EMPTY;
        }
        boolean canEarlyTerminate = false;
        final FieldContext fieldContext = config.fieldContext();
        if (sortField != null && config.fieldContext() != null) {
            canEarlyTerminate = checkCanEarlyTerminate(context.searcher().getIndexReader(),
                fieldContext.field(), order() == SortOrder.ASC ? false : true, sortField);
        }
        return new CompositeValuesSourceConfig(name, vs, order(), canEarlyTerminate);
    }
}
