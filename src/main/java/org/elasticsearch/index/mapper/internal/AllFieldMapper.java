/*
 * Licensed to ElasticSearch and Shay Banon under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. ElasticSearch licenses this
 * file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
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

package org.elasticsearch.index.mapper.internal;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.Fieldable;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.TermQuery;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.lucene.Lucene;
import org.elasticsearch.common.lucene.all.AllField;
import org.elasticsearch.common.lucene.all.AllTermQuery;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.index.analysis.NamedAnalyzer;
import org.elasticsearch.index.mapper.*;
import org.elasticsearch.index.mapper.core.AbstractFieldMapper;
import org.elasticsearch.index.query.QueryParseContext;

import java.io.IOException;
import java.util.Map;

import static org.elasticsearch.common.xcontent.support.XContentMapValues.nodeBooleanValue;
import static org.elasticsearch.index.mapper.MapperBuilders.all;
import static org.elasticsearch.index.mapper.core.TypeParsers.parseField;

/**
 *
 */
public class AllFieldMapper extends AbstractFieldMapper<Void> implements InternalMapper, RootMapper {

    public interface IncludeInAll extends Mapper {

        void includeInAll(Boolean includeInAll);

        void includeInAllIfNotSet(Boolean includeInAll);
    }

    public static final String NAME = "_all";

    public static final String CONTENT_TYPE = "_all";

    public static class Defaults extends AbstractFieldMapper.Defaults {
        public static final String NAME = AllFieldMapper.NAME;
        public static final String INDEX_NAME = AllFieldMapper.NAME;
        public static final boolean ENABLED = true;
    }

    public static class Builder extends AbstractFieldMapper.Builder<Builder, AllFieldMapper> {

        private boolean enabled = Defaults.ENABLED;

        // an internal flag, automatically set if we encounter boosting
        boolean autoBoost = false;

        public Builder() {
            super(Defaults.NAME);
            builder = this;
            indexName = Defaults.INDEX_NAME;
        }

        public Builder enabled(boolean enabled) {
            this.enabled = enabled;
            return this;
        }

        @Override
        public Builder store(Field.Store store) {
            return super.store(store);
        }

        @Override
        public Builder termVector(Field.TermVector termVector) {
            return super.termVector(termVector);
        }

        @Override
        protected Builder indexAnalyzer(NamedAnalyzer indexAnalyzer) {
            return super.indexAnalyzer(indexAnalyzer);
        }

        @Override
        protected Builder searchAnalyzer(NamedAnalyzer searchAnalyzer) {
            return super.searchAnalyzer(searchAnalyzer);
        }

        @Override
        public AllFieldMapper build(BuilderContext context) {
            return new AllFieldMapper(name, store, termVector, omitNorms, omitTermFreqAndPositions,
                    indexAnalyzer, searchAnalyzer, enabled, autoBoost);
        }
    }

    public static class TypeParser implements Mapper.TypeParser {
        @Override
        public Mapper.Builder parse(String name, Map<String, Object> node, ParserContext parserContext) throws MapperParsingException {
            AllFieldMapper.Builder builder = all();
            parseField(builder, builder.name, node, parserContext);
            for (Map.Entry<String, Object> entry : node.entrySet()) {
                String fieldName = Strings.toUnderscoreCase(entry.getKey());
                Object fieldNode = entry.getValue();
                if (fieldName.equals("enabled")) {
                    builder.enabled(nodeBooleanValue(fieldNode));
                } else if (fieldName.equals("auto_boost")) {
                    builder.autoBoost = nodeBooleanValue(fieldNode);
                }
            }
            return builder;
        }
    }


    private boolean enabled;
    // The autoBoost flag is automatically set based on indexed docs on the mappings
    // if a doc is indexed with a specific boost value and part of _all, it is automatically
    // set to true. This allows to optimize (automatically, which we like) for the common case
    // where fields don't usually have boost associated with them, and we don't need to use the
    // special SpanTermQuery to look at payloads
    private volatile boolean autoBoost;

    public AllFieldMapper() {
        this(Defaults.NAME, Defaults.STORE, Defaults.TERM_VECTOR, Defaults.OMIT_NORMS, Defaults.OMIT_TERM_FREQ_AND_POSITIONS, null, null, Defaults.ENABLED, false);
    }

    protected AllFieldMapper(String name, Field.Store store, Field.TermVector termVector, boolean omitNorms, boolean omitTermFreqAndPositions,
                             NamedAnalyzer indexAnalyzer, NamedAnalyzer searchAnalyzer, boolean enabled, boolean autoBoost) {
        super(new Names(name, name, name, name), Field.Index.ANALYZED, store, termVector, 1.0f, omitNorms, omitTermFreqAndPositions,
                indexAnalyzer, searchAnalyzer);
        this.enabled = enabled;
        this.autoBoost = autoBoost;

    }

    public boolean enabled() {
        return this.enabled;
    }

    @Override
    public Query queryStringTermQuery(Term term) {
        if (!autoBoost) {
            return new TermQuery(term);
        }
        return new AllTermQuery(term);
    }

    @Override
    public Query fieldQuery(String value, QueryParseContext context) {
        return queryStringTermQuery(names().createIndexNameTerm(value));
    }

    @Override
    public void preParse(ParseContext context) throws IOException {
    }

    @Override
    public void postParse(ParseContext context) throws IOException {
        super.parse(context);
    }

    @Override
    public void parse(ParseContext context) throws IOException {
        // we parse in post parse
    }

    @Override
    public void validate(ParseContext context) throws MapperParsingException {
    }

    @Override
    public boolean includeInObject() {
        return true;
    }

    @Override
    protected Fieldable parseCreateField(ParseContext context) throws IOException {
        if (!enabled) {
            return null;
        }
        // reset the entries
        context.allEntries().reset();

        // if the autoBoost flag is not set, and we indexed a doc with custom boost, make
        // sure to update the flag, and notify mappings on change
        if (!autoBoost && context.allEntries().customBoost()) {
            autoBoost = true;
            context.setMappingsModified();
        }

        Analyzer analyzer = findAnalyzer(context);
        return new AllField(names.indexName(), store, termVector, context.allEntries(), analyzer);
    }

    private Analyzer findAnalyzer(ParseContext context) {
        Analyzer analyzer = indexAnalyzer;
        if (analyzer == null) {
            analyzer = context.analyzer();
            if (analyzer == null) {
                analyzer = context.docMapper().indexAnalyzer();
                if (analyzer == null) {
                    // This should not happen, should we log warn it?
                    analyzer = Lucene.STANDARD_ANALYZER;
                }
            }
        }
        return analyzer;
    }

    @Override
    public Void value(Fieldable field) {
        return null;
    }

    @Override
    public Void valueFromString(String value) {
        return null;
    }

    @Override
    public String valueAsString(Fieldable field) {
        return null;
    }

    @Override
    public Object valueForSearch(Fieldable field) {
        return null;
    }

    @Override
    protected String contentType() {
        return CONTENT_TYPE;
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        // if all are defaults, no need to write it at all
        if (enabled == Defaults.ENABLED && store == Defaults.STORE && termVector == Defaults.TERM_VECTOR && indexAnalyzer == null && searchAnalyzer == null) {
            return builder;
        }
        builder.startObject(CONTENT_TYPE);
        if (enabled != Defaults.ENABLED) {
            builder.field("enabled", enabled);
        }
        if (autoBoost != false) {
            builder.field("auto_boost", autoBoost);
        }
        if (store != Defaults.STORE) {
            builder.field("store", store.name().toLowerCase());
        }
        if (termVector != Defaults.TERM_VECTOR) {
            builder.field("term_vector", termVector.name().toLowerCase());
        }
        if (indexAnalyzer != null && searchAnalyzer != null && indexAnalyzer.name().equals(searchAnalyzer.name()) && !indexAnalyzer.name().startsWith("_")) {
            // same analyzers, output it once
            builder.field("analyzer", indexAnalyzer.name());
        } else {
            if (indexAnalyzer != null && !indexAnalyzer.name().startsWith("_")) {
                builder.field("index_analyzer", indexAnalyzer.name());
            }
            if (searchAnalyzer != null && !searchAnalyzer.name().startsWith("_")) {
                builder.field("search_analyzer", searchAnalyzer.name());
            }
        }
        builder.endObject();
        return builder;
    }

    @Override
    public void merge(Mapper mergeWith, MergeContext mergeContext) throws MergeMappingException {
        // do nothing here, no merging, but also no exception
    }
}
