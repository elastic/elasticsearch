/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.search.fetch.subphase.highlight.constantkeyword;

import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.TermsEnum;
import org.apache.lucene.search.MatchAllDocsQuery;
import org.apache.lucene.search.MatchNoDocsQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.common.regex.Regex;
import org.elasticsearch.index.fielddata.FieldData;
import org.elasticsearch.index.fielddata.FieldDataContext;
import org.elasticsearch.index.fielddata.IndexFieldData;
import org.elasticsearch.index.fielddata.plain.ConstantIndexFieldData;
import org.elasticsearch.index.mapper.ConstantFieldType;
import org.elasticsearch.index.mapper.KeywordFieldMapper;
import org.elasticsearch.index.mapper.ValueFetcher;
import org.elasticsearch.index.query.SearchExecutionContext;
import org.elasticsearch.script.field.KeywordDocValuesField;
import org.elasticsearch.search.aggregations.support.CoreValuesSourceType;

import java.util.Locale;
import java.util.Map;

public class MockConstantFieldType extends ConstantFieldType {

    public static final String CONTENT_TYPE = "constant_keyword";
    public final String value;

    public MockConstantFieldType(String name, String value, Map<String, String> meta) {
        super(name, meta);
        this.value = value;
    }

    public String value() {
        return value;
    }

    @Override
    public String typeName() {
        return CONTENT_TYPE;
    }

    @Override
    public String familyTypeName() {
        return KeywordFieldMapper.CONTENT_TYPE;
    }

    @Override
    public IndexFieldData.Builder fielddataBuilder(FieldDataContext fieldDataContext) {
        return new ConstantIndexFieldData.Builder(
            value,
            name(),
            CoreValuesSourceType.KEYWORD,
            (dv, n) -> new KeywordDocValuesField(FieldData.toString(dv), n)
        );
    }

    @Override
    public ValueFetcher valueFetcher(SearchExecutionContext context, String format) {
        if (format != null) {
            throw new IllegalArgumentException("Field [" + name() + "] of type [" + typeName() + "] doesn't support formats.");
        }

        return value == null ? ValueFetcher.EMPTY : ValueFetcher.singleton(value);
    }

    @Override
    public Object valueForDisplay(Object value) {
        if (value == null) {
            return null;
        }
        BytesRef binaryValue = (BytesRef) value;
        return binaryValue.utf8ToString();
    }

    @Override
    public TermsEnum getTerms(IndexReader reader, String prefix, boolean caseInsensitive, String searchAfter) {
        if (value == null) {
            return TermsEnum.EMPTY;
        }
        boolean matches = caseInsensitive
            ? value.toLowerCase(Locale.ROOT).startsWith(prefix.toLowerCase(Locale.ROOT))
            : value.startsWith(prefix);
        if (matches == false) {
            return TermsEnum.EMPTY;
        }
        if (searchAfter != null) {
            if (searchAfter.compareTo(value) >= 0) {
                // The constant value is before the searchAfter value so must be ignored
                return TermsEnum.EMPTY;
            }
        }
        return TermsEnum.EMPTY;
    }

    @Override
    protected boolean matches(String pattern, boolean caseInsensitive, SearchExecutionContext context) {
        if (value == null) {
            return false;
        }
        return Regex.simpleMatch(pattern, value, caseInsensitive);
    }

    @Override
    public Query existsQuery(SearchExecutionContext context) {
        return value != null ? new MatchAllDocsQuery() : new MatchNoDocsQuery();
    }
}
