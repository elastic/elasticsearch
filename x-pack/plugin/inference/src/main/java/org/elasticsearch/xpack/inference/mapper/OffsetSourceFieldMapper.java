/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.mapper;

import org.apache.lucene.index.FieldInfos;
import org.apache.lucene.index.PostingsEnum;
import org.apache.lucene.index.Term;
import org.apache.lucene.index.Terms;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.search.PrefixQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.automaton.Automaton;
import org.apache.lucene.util.automaton.CompiledAutomaton;
import org.elasticsearch.common.xcontent.XContentParserUtils;
import org.elasticsearch.index.fielddata.FieldDataContext;
import org.elasticsearch.index.fielddata.IndexFieldData;
import org.elasticsearch.index.mapper.DocumentParserContext;
import org.elasticsearch.index.mapper.FieldMapper;
import org.elasticsearch.index.mapper.MappedFieldType;
import org.elasticsearch.index.mapper.MapperBuilderContext;
import org.elasticsearch.index.mapper.TextSearchInfo;
import org.elasticsearch.index.mapper.ValueFetcher;
import org.elasticsearch.index.query.SearchExecutionContext;
import org.elasticsearch.xcontent.XContentParser;

import java.io.IOException;
import java.util.LinkedHashMap;
import java.util.Map;

public class OffsetSourceFieldMapper extends FieldMapper {
    public static final String CONTENT_TYPE = "offset_source";
    public static final String NAME = "_offset_source";

    private static final String SOURCE_NAME_FIELD = "field";
    private static final String START_OFFSET_FIELD = "start";
    private static final String END_OFFSET_FIELD = "end";

    public static class Builder extends FieldMapper.Builder {
        private final Parameter<Map<String, String>> meta = Parameter.metaParam();

        public Builder(String name) {
            super(name);
        }

        @Override
        protected Parameter<?>[] getParameters() {
            return new Parameter<?>[] { meta };
        }

        @Override
        public OffsetSourceFieldMapper build(MapperBuilderContext context) {
            return new OffsetSourceFieldMapper(
                leafName(),
                new OffsetSourceFieldType(context.buildFullName(leafName()), meta.getValue()),
                builderParams(this, context)
            );
        }
    }

    public static final TypeParser PARSER = new TypeParser((n, c) -> new Builder(n));

    public static final class OffsetSourceFieldType extends MappedFieldType {
        public OffsetSourceFieldType(String name, Map<String, String> meta) {
            super(name, true, false, false, TextSearchInfo.NONE, meta);
        }

        @Override
        public String typeName() {
            return CONTENT_TYPE;
        }

        @Override
        public Query existsQuery(SearchExecutionContext context) {
            return new TermQuery(new Term(NAME, name()));
        }

        @Override
        public boolean fieldHasValue(FieldInfos fieldInfos) {
            return fieldInfos.fieldInfo(NAME) != null;
        }

        @Override
        public IndexFieldData.Builder fielddataBuilder(FieldDataContext fieldDataContext) {
            throw new IllegalArgumentException("[rank_feature] fields do not support sorting, scripting or aggregating");
        }

        @Override
        public ValueFetcher valueFetcher(SearchExecutionContext context, String format) {
            return ValueFetcher.EMPTY;
        }

        @Override
        public Query termQuery(Object value, SearchExecutionContext context) {
            throw new IllegalArgumentException("Queries on [offset] fields are not supported");
        }
    }

    /**
     * @param simpleName      the leaf name of the mapper
     * @param mappedFieldType
     * @param params          initialization params for this field mapper
     */
    protected OffsetSourceFieldMapper(String simpleName, MappedFieldType mappedFieldType, BuilderParams params) {
        super(simpleName, mappedFieldType, params);
    }

    @Override
    protected String contentType() {
        return CONTENT_TYPE;
    }

    @Override
    protected boolean supportsParsingObject() {
        return true;
    }

    @Override
    protected void parseCreateField(DocumentParserContext context) throws IOException {
        XContentParser parser = context.parser();
        boolean isWithinLeafObject = context.path().isWithinLeafObject();
        // make sure that we don't expand dots in field names while parsing
        context.path().setWithinLeafObject(true);
        try {
            XContentParserUtils.ensureExpectedToken(XContentParser.Token.START_OBJECT, parser.currentToken(), parser);
            String fieldName = null;
            String sourceFieldName = null;
            int startOffset = -1;
            int endOffset = -1;
            while (parser.nextToken() != XContentParser.Token.END_OBJECT) {
                if (parser.currentToken() == XContentParser.Token.FIELD_NAME) {
                    fieldName = parser.currentName();
                } else if (SOURCE_NAME_FIELD.equals(fieldName)) {
                    sourceFieldName = parser.text();
                } else if (START_OFFSET_FIELD.equals(fieldName)) {
                    startOffset = parser.intValue();
                } else if (END_OFFSET_FIELD.equals(fieldName)) {
                    endOffset = parser.intValue();
                } else {
                    throw new IllegalArgumentException("Unkown field name [" + fieldName + "]");
                }
            }
            context.doc().addWithKey(fullPath(), new OffsetSourceField(NAME, fullPath() + "." + sourceFieldName, startOffset, endOffset));
        } finally {
            context.path().setWithinLeafObject(isWithinLeafObject);
        }
    }

    @Override
    public FieldMapper.Builder getMergeBuilder() {
        return new Builder(leafName()).init(this);
    }

    public static class OffsetsLoader {
        private final String fieldName;
        private final Map<String, PostingsEnum> postingsEnums = new LinkedHashMap<>();
        private String sourceFieldName;
        private int startOffset;
        private int endOffset;

        public OffsetsLoader(String fieldName, Terms terms) throws IOException {
            this.fieldName = fieldName;
            Automaton prefixAutomaton = PrefixQuery.toAutomaton(new BytesRef(fieldName + "."));
            var termsEnum = terms.intersect(new CompiledAutomaton(prefixAutomaton, false, true, false), null);
            while (termsEnum.next() != null) {
                var postings = termsEnum.postings(null, PostingsEnum.OFFSETS);
                if (postings.nextDoc() != DocIdSetIterator.NO_MORE_DOCS) {
                    String sourceFieldName = termsEnum.term().utf8ToString().substring(fieldName.length() + 1);
                    postingsEnums.put(sourceFieldName, postings);
                }
            }
        }

        public boolean advanceTo(int doc) throws IOException {
            for (var it = postingsEnums.entrySet().iterator(); it.hasNext();) {
                var entry = it.next();
                var postings = entry.getValue();
                if (postings.docID() < doc) {
                    if (postings.advance(doc) == DocIdSetIterator.NO_MORE_DOCS) {
                        it.remove();
                        continue;
                    }
                }
                if (postings.docID() == doc) {
                    assert postings.freq() == 1;
                    postings.nextPosition();
                    sourceFieldName = entry.getKey();
                    startOffset = postings.startOffset();
                    endOffset = postings.endOffset();
                    return true;
                }
            }
            return false;
        }

        public String getSourceFieldName() {
            return sourceFieldName;
        }

        public int getStartOffset() {
            return startOffset;
        }

        public int getEndOffset() {
            return endOffset;
        }
    }
}
