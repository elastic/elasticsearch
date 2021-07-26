/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.cluster.routing.allocation.mapper;

import org.apache.lucene.search.MatchAllDocsQuery;
import org.apache.lucene.search.MatchNoDocsQuery;
import org.apache.lucene.search.Query;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.regex.Regex;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.mapper.ConstantFieldType;
import org.elasticsearch.index.mapper.KeywordFieldMapper;
import org.elasticsearch.index.mapper.MetadataFieldMapper;
import org.elasticsearch.index.mapper.ValueFetcher;
import org.elasticsearch.index.query.SearchExecutionContext;
import org.elasticsearch.xpack.cluster.routing.allocation.DataTierAllocationDecider;

import java.util.Collections;
import java.util.List;

public class DataTierFieldMapper extends MetadataFieldMapper {

    public static final String NAME = "_tier";

    public static final String CONTENT_TYPE = "_tier";

    public static final TypeParser PARSER = new FixedTypeParser(c -> new DataTierFieldMapper());

    static final class DataTierFieldType extends ConstantFieldType {

        static final DataTierFieldType INSTANCE = new DataTierFieldType();

        private DataTierFieldType() {
            super(NAME, Collections.emptyMap());
        }

        @Override
        public String typeName() {
            return CONTENT_TYPE;
        }

        @Override
        public boolean isMetadataField() {
            return true;
        }

        @Override
        public String familyTypeName() {
            return KeywordFieldMapper.CONTENT_TYPE;
        }

        @Override
        protected boolean matches(String pattern, boolean caseInsensitive, SearchExecutionContext context) {
            if (caseInsensitive) {
                pattern = Strings.toLowercaseAscii(pattern);
            }

            String tierPreference = getTierPreference(context);
            if (tierPreference == null) {
                return false;
            }
            return Regex.simpleMatch(pattern, tierPreference);
        }

        @Override
        public Query existsQuery(SearchExecutionContext context) {
            String tierPreference = getTierPreference(context);
            if (tierPreference == null) {
                return new MatchNoDocsQuery();
            }
            return new MatchAllDocsQuery();
        }

        @Override
        public ValueFetcher valueFetcher(SearchExecutionContext context, String format) {
            if (format != null) {
                throw new IllegalArgumentException("Field [" + name() + "] of type [" + typeName() + "] doesn't support formats.");
            }

            String tierPreference = getTierPreference(context);
            return tierPreference == null
                ? lookup -> List.of()
                : lookup -> List.of(tierPreference);
        }

        /**
         * Retrieve the first tier preference from the index setting. If the setting is not
         * present, then return null.
         */
        private String getTierPreference(SearchExecutionContext context) {
            Settings settings = context.getIndexSettings().getSettings();
            String value = DataTierAllocationDecider.INDEX_ROUTING_PREFER_SETTING.get(settings);

            if (Strings.hasText(value) == false) {
                return null;
            }

            // Tier preference can be a comma-delimited list of tiers, ordered by preference
            // It was decided we should only test the first of these potentially multiple preferences.
            return value.split(",")[0].trim();
        }
    }

    public DataTierFieldMapper() {
        super(DataTierFieldType.INSTANCE);
    }

    @Override
    protected String contentType() {
        return CONTENT_TYPE;
    }
}
