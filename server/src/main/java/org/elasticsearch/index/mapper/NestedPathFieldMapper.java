/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.index.mapper;

import org.apache.lucene.document.Field;
import org.apache.lucene.document.StringField;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.Version;
import org.elasticsearch.index.query.SearchExecutionContext;

import java.util.Collections;

public class NestedPathFieldMapper extends MetadataFieldMapper {

    public static final String NAME_PRE_V8 = "_type";

    public static final String NAME = "_nested_path";

    private static final NestedPathFieldMapper INSTANCE = new NestedPathFieldMapper(NAME);
    private static final NestedPathFieldMapper INSTANCE_PRE_V8 = new NestedPathFieldMapper(NAME_PRE_V8);

    public static String name(Version version) {
        if (version.before(Version.V_8_0_0)) {
            return NAME_PRE_V8;
        }
        return NAME;
    }

    public static Query filter(Version version, String path) {
        return new TermQuery(new Term(name(version), new BytesRef(path)));
    }

    public static Field field(Version version, String path) {
        return new StringField(name(version), path, Field.Store.NO);
    }

    public static final TypeParser PARSER = new FixedTypeParser(
        c -> c.indexVersionCreated().before(Version.V_8_0_0) ? INSTANCE_PRE_V8 : INSTANCE
    );

    public static final class NestedPathFieldType extends StringFieldType {

        private NestedPathFieldType(String name) {
            super(name, true, false, false, TextSearchInfo.SIMPLE_MATCH_ONLY, Collections.emptyMap());
        }

        @Override
        public String typeName() {
            return NAME;
        }

        @Override
        public Query existsQuery(SearchExecutionContext context) {
            throw new UnsupportedOperationException("Cannot run exists() query against the nested field path");
        }

        @Override
        public ValueFetcher valueFetcher(SearchExecutionContext context, String format) {
            throw new UnsupportedOperationException("Cannot fetch values for internal field [" + name() + "].");
        }

        @Override
        public boolean mayExistInIndex(SearchExecutionContext context) {
            return false;
        }
    }

    private NestedPathFieldMapper(String name) {
        super(new NestedPathFieldType(name));
    }

    @Override
    protected String contentType() {
        return NAME;
    }

    @Override
    public SourceLoader.SyntheticFieldLoader syntheticFieldLoader() {
        return SourceLoader.SyntheticFieldLoader.NOTHING;
    }
}
