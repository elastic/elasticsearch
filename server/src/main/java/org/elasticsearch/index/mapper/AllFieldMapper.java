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

package org.elasticsearch.index.mapper;

import org.apache.lucene.document.FieldType;
import org.apache.lucene.index.IndexOptions;
import org.apache.lucene.search.MatchNoDocsQuery;
import org.apache.lucene.search.Query;
import org.elasticsearch.common.Explicit;
import org.elasticsearch.index.query.QueryShardContext;
import org.elasticsearch.search.lookup.SearchLookup;

import java.util.Collections;
import java.util.List;

/**
 * Noop mapper that ensures that mappings created in 6x that explicitly disable the _all field
 * can be restored in this version.
 *
 * TODO: Remove in 8
 */
public class AllFieldMapper extends MetadataFieldMapper {
    public static final String NAME = "_all";
    public static final String CONTENT_TYPE = "_all";

    public static class Defaults {
        public static final FieldType FIELD_TYPE = new FieldType();

        static {
            FIELD_TYPE.setIndexOptions(IndexOptions.DOCS_AND_FREQS_AND_POSITIONS);
            FIELD_TYPE.setTokenized(true);
            FIELD_TYPE.freeze();
        }
    }

    private static AllFieldMapper toType(FieldMapper in) {
        return (AllFieldMapper) in;
    }

    public static class Builder extends MetadataFieldMapper.Builder {

        private final Parameter<Explicit<Boolean>> enabled = updateableBoolParam("enabled", m -> toType(m).enabled, false);

        public Builder() {
            super(NAME);
        }

        @Override
        protected List<Parameter<?>> getParameters() {
            return Collections.singletonList(enabled);
        }

        @Override
        public AllFieldMapper build(BuilderContext context) {
            if (enabled.getValue().value()) {
                throw new IllegalArgumentException("[_all] is disabled in this version.");
            }
            return new AllFieldMapper(enabled.getValue());
        }
    }

    public static final TypeParser PARSER = new ConfigurableTypeParser(
        c -> new AllFieldMapper(new Explicit<>(false, false)),
        c -> new Builder()
    );

    static final class AllFieldType extends StringFieldType {
        AllFieldType() {
            super(NAME, false, false, false, TextSearchInfo.NONE, Collections.emptyMap());
        }

        @Override
        public ValueFetcher valueFetcher(MapperService mapperService, SearchLookup searchLookup, String format) {
            throw new UnsupportedOperationException();
        }

        @Override
        public String typeName() {
            return CONTENT_TYPE;
        }

        @Override
        public Query existsQuery(QueryShardContext context) {
            return new MatchNoDocsQuery();
        }
    }

    private final Explicit<Boolean> enabled;

    private AllFieldMapper(Explicit<Boolean> enabled) {
        super(new AllFieldType());
        this.enabled = enabled;
    }

    @Override
    protected String contentType() {
        return CONTENT_TYPE;
    }

    @Override
    public ParametrizedFieldMapper.Builder getMergeBuilder() {
        return new Builder().init(this);
    }
}
