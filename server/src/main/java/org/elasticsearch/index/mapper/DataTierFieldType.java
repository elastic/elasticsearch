/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.mapper;

import org.apache.lucene.search.MatchAllDocsQuery;
import org.apache.lucene.search.MatchNoDocsQuery;
import org.apache.lucene.search.Query;
import org.elasticsearch.cluster.routing.allocation.DataTier;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.regex.Regex;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.query.QueryRewriteContext;
import org.elasticsearch.index.query.SearchExecutionContext;

import java.util.Collections;

public final class DataTierFieldType extends ConstantFieldType {

    public static final DataTierFieldType INSTANCE = new DataTierFieldType();
    public static final String NAME = "_tier";

    public static final String CONTENT_TYPE = "_tier";

    private DataTierFieldType() {
        super(NAME, Collections.emptyMap());
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
    protected boolean matches(String pattern, boolean caseInsensitive, QueryRewriteContext context) {
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
        return tierPreference == null ? ValueFetcher.EMPTY : ValueFetcher.singleton(tierPreference);
    }

    /**
     * Retrieve the first tier preference from the index setting. If the setting is not
     * present, then return null.
     */
    private static String getTierPreference(QueryRewriteContext context) {
        Settings settings = context.getIndexSettings().getSettings();
        String value = DataTier.TIER_PREFERENCE_SETTING.get(settings);

        if (Strings.hasText(value) == false) {
            return null;
        }

        // Tier preference can be a comma-delimited list of tiers, ordered by preference
        // It was decided we should only test the first of these potentially multiple preferences.
        return value.split(",")[0].trim();
    }
}
