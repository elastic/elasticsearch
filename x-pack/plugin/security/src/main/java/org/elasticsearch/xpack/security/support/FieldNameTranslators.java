/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.security.support;

import org.elasticsearch.common.regex.Regex;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.core.Strings;
import org.elasticsearch.index.query.AbstractQueryBuilder;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.ExistsQueryBuilder;
import org.elasticsearch.index.query.IdsQueryBuilder;
import org.elasticsearch.index.query.MatchAllQueryBuilder;
import org.elasticsearch.index.query.MatchNoneQueryBuilder;
import org.elasticsearch.index.query.MatchQueryBuilder;
import org.elasticsearch.index.query.PrefixQueryBuilder;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.index.query.RangeQueryBuilder;
import org.elasticsearch.index.query.SimpleQueryStringBuilder;
import org.elasticsearch.index.query.TermQueryBuilder;
import org.elasticsearch.index.query.TermsQueryBuilder;
import org.elasticsearch.index.query.WildcardQueryBuilder;
import org.elasticsearch.index.search.QueryParserHelper;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.search.sort.FieldSortBuilder;

import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.function.Consumer;

import static org.elasticsearch.xpack.security.action.apikey.TransportQueryApiKeyAction.API_KEY_TYPE_RUNTIME_MAPPING_FIELD;
import static org.elasticsearch.xpack.security.action.role.TransportQueryRoleAction.ROLE_NAME_RUNTIME_MAPPING_FIELD;

public final class FieldNameTranslators {

    public static final String FLATTENED_METADATA_INDEX_FIELD_NAME = "metadata_flattened";

    public static final FieldNameTranslators API_KEY_FIELD_NAME_TRANSLATORS = new FieldNameTranslators(
        List.of(
            new SimpleFieldNameTranslator("creator.principal", "username"),
            new SimpleFieldNameTranslator("creator.realm", "realm_name"),
            new SimpleFieldNameTranslator("name", "name"),
            new SimpleFieldNameTranslator(API_KEY_TYPE_RUNTIME_MAPPING_FIELD, "type"),
            new SimpleFieldNameTranslator("creation_time", "creation"),
            new SimpleFieldNameTranslator("expiration_time", "expiration"),
            new SimpleFieldNameTranslator("api_key_invalidated", "invalidated"),
            new SimpleFieldNameTranslator("invalidation_time", "invalidation"),
            // allows querying on any non-wildcard sub-fields under the "metadata." prefix
            // also allows querying on the "metadata" field itself (including by specifying patterns)
            new FlattenedFieldNameTranslator(FLATTENED_METADATA_INDEX_FIELD_NAME, "metadata")
        )
    );

    public static final FieldNameTranslators USER_FIELD_NAME_TRANSLATORS = new FieldNameTranslators(
        List.of(
            idemFieldNameTranslator("username"),
            idemFieldNameTranslator("roles"),
            idemFieldNameTranslator("enabled"),
            // the mapping for these fields does not support sorting (because their mapping does not store "fielddata" in the index)
            idemFieldNameTranslator("full_name", false),
            idemFieldNameTranslator("email", false)
        )
    );

    public static final FieldNameTranslators ROLE_FIELD_NAME_TRANSLATORS = new FieldNameTranslators(
        List.of(
            new SimpleFieldNameTranslator(ROLE_NAME_RUNTIME_MAPPING_FIELD, "name"),
            idemFieldNameTranslator("description"),
            idemFieldNameTranslator("applications.application"),
            idemFieldNameTranslator("applications.resources"),
            idemFieldNameTranslator("applications.privileges"),
            // allows querying on any non-wildcard sub-fields under the "metadata." prefix
            // also allows querying on the "metadata" field itself (including by specifying patterns)
            new FlattenedFieldNameTranslator(FLATTENED_METADATA_INDEX_FIELD_NAME, "metadata")
        )
    );

    private final List<FieldNameTranslator> fieldNameTranslators;

    private FieldNameTranslators(List<FieldNameTranslator> fieldNameTranslators) {
        this.fieldNameTranslators = fieldNameTranslators;
    }

    /**
     * Deep copies the passed-in {@param queryBuilder} translating all the field names, from query level to index level,
     * see {@link  #translate}. In general, the returned builder should create the same query as if the query were
     * created by the passed in {@param queryBuilder}, only with the field names translated.
     * Field name patterns (including "*"), are also replaced with the explicit index level field names whose
     * associated query level field names match the pattern.
     * The optional {@param visitor} can be used to collect all the translated field names.
     */
    public QueryBuilder translateQueryBuilderFields(QueryBuilder queryBuilder, @Nullable Consumer<String> visitor) {
        Objects.requireNonNull(queryBuilder, "unsupported \"null\" query builder for field name translation");
        final Consumer<String> fieldNameVisitor = visitor != null ? visitor : ignored -> {};
        if (queryBuilder instanceof final BoolQueryBuilder query) {
            final BoolQueryBuilder newQuery = QueryBuilders.boolQuery()
                .minimumShouldMatch(query.minimumShouldMatch())
                .adjustPureNegative(query.adjustPureNegative())
                .boost(query.boost())
                .queryName(query.queryName());
            query.must().stream().map(q -> translateQueryBuilderFields(q, fieldNameVisitor)).forEach(newQuery::must);
            query.should().stream().map(q -> translateQueryBuilderFields(q, fieldNameVisitor)).forEach(newQuery::should);
            query.mustNot().stream().map(q -> translateQueryBuilderFields(q, fieldNameVisitor)).forEach(newQuery::mustNot);
            query.filter().stream().map(q -> translateQueryBuilderFields(q, fieldNameVisitor)).forEach(newQuery::filter);
            return newQuery;
        } else if (queryBuilder instanceof final MatchAllQueryBuilder query) {
            // just be safe and consistent to always return a new copy instance of the translated query builders
            return QueryBuilders.matchAllQuery().boost(query.boost()).queryName(query.queryName());
        } else if (queryBuilder instanceof final IdsQueryBuilder query) {
            // just be safe and consistent to always return a new copy instance of the translated query builders
            return QueryBuilders.idsQuery().addIds(query.ids().toArray(new String[0])).boost(query.boost()).queryName(query.queryName());
        } else if (queryBuilder instanceof final TermQueryBuilder query) {
            final String translatedFieldName = translate(query.fieldName());
            fieldNameVisitor.accept(translatedFieldName);
            return QueryBuilders.termQuery(translatedFieldName, query.value())
                .caseInsensitive(query.caseInsensitive())
                .boost(query.boost())
                .queryName(query.queryName());
        } else if (queryBuilder instanceof final ExistsQueryBuilder query) {
            final String translatedFieldName = translate(query.fieldName());
            fieldNameVisitor.accept(translatedFieldName);
            return QueryBuilders.existsQuery(translatedFieldName).boost(query.boost()).queryName(query.queryName());
        } else if (queryBuilder instanceof final TermsQueryBuilder query) {
            if (query.termsLookup() != null) {
                throw new IllegalArgumentException("terms query with terms lookup is not currently supported in this context");
            }
            final String translatedFieldName = translate(query.fieldName());
            fieldNameVisitor.accept(translatedFieldName);
            return QueryBuilders.termsQuery(translatedFieldName, query.getValues()).boost(query.boost()).queryName(query.queryName());
        } else if (queryBuilder instanceof final PrefixQueryBuilder query) {
            final String translatedFieldName = translate(query.fieldName());
            fieldNameVisitor.accept(translatedFieldName);
            return QueryBuilders.prefixQuery(translatedFieldName, query.value())
                .caseInsensitive(query.caseInsensitive())
                .rewrite(query.rewrite())
                .boost(query.boost())
                .queryName(query.queryName());
        } else if (queryBuilder instanceof final WildcardQueryBuilder query) {
            final String translatedFieldName = translate(query.fieldName());
            fieldNameVisitor.accept(translatedFieldName);
            return QueryBuilders.wildcardQuery(translatedFieldName, query.value())
                .caseInsensitive(query.caseInsensitive())
                .rewrite(query.rewrite())
                .boost(query.boost())
                .queryName(query.queryName());
        } else if (queryBuilder instanceof final MatchQueryBuilder query) {
            final String translatedFieldName = translate(query.fieldName());
            fieldNameVisitor.accept(translatedFieldName);
            final MatchQueryBuilder matchQueryBuilder = QueryBuilders.matchQuery(translatedFieldName, query.value());
            if (query.operator() != null) {
                matchQueryBuilder.operator(query.operator());
            }
            if (query.analyzer() != null) {
                matchQueryBuilder.analyzer(query.analyzer());
            }
            if (query.fuzziness() != null) {
                matchQueryBuilder.fuzziness(query.fuzziness());
            }
            if (query.minimumShouldMatch() != null) {
                matchQueryBuilder.minimumShouldMatch(query.minimumShouldMatch());
            }
            if (query.fuzzyRewrite() != null) {
                matchQueryBuilder.fuzzyRewrite(query.fuzzyRewrite());
            }
            if (query.zeroTermsQuery() != null) {
                matchQueryBuilder.zeroTermsQuery(query.zeroTermsQuery());
            }
            matchQueryBuilder.prefixLength(query.prefixLength())
                .maxExpansions(query.maxExpansions())
                .fuzzyTranspositions(query.fuzzyTranspositions())
                .lenient(query.lenient())
                .autoGenerateSynonymsPhraseQuery(query.autoGenerateSynonymsPhraseQuery())
                .boost(query.boost())
                .queryName(query.queryName());
            return matchQueryBuilder;
        } else if (queryBuilder instanceof final RangeQueryBuilder query) {
            if (query.relation() != null) {
                throw new IllegalArgumentException("range query with relation is not currently supported in this context");
            }
            final String translatedFieldName = translate(query.fieldName());
            fieldNameVisitor.accept(translatedFieldName);
            final RangeQueryBuilder newQuery = QueryBuilders.rangeQuery(translatedFieldName);
            if (query.format() != null) {
                newQuery.format(query.format());
            }
            if (query.timeZone() != null) {
                newQuery.timeZone(query.timeZone());
            }
            if (query.from() != null) {
                newQuery.from(query.from()).includeLower(query.includeLower());
            }
            if (query.to() != null) {
                newQuery.to(query.to()).includeUpper(query.includeUpper());
            }
            return newQuery.boost(query.boost()).queryName(query.queryName());
        } else if (queryBuilder instanceof final SimpleQueryStringBuilder query) {
            SimpleQueryStringBuilder simpleQueryStringBuilder = QueryBuilders.simpleQueryStringQuery(query.value());
            Map<String, Float> queryFields = new HashMap<>(query.fields());
            // be explicit that no field means all fields
            if (queryFields.isEmpty()) {
                queryFields.put("*", AbstractQueryBuilder.DEFAULT_BOOST);
            }
            // override "lenient" if querying all the fields, because, due to different field mappings,
            // the query parsing will almost certainly fail otherwise
            if (QueryParserHelper.hasAllFieldsWildcard(queryFields.keySet())) {
                simpleQueryStringBuilder.lenient(true);
            } else {
                simpleQueryStringBuilder.lenient(query.lenient());
            }
            // translate query-level field name patterns to index-level concrete field names
            for (Map.Entry<String, Float> requestedFieldNameOrPattern : queryFields.entrySet()) {
                for (String translatedField : translatePattern(requestedFieldNameOrPattern.getKey())) {
                    simpleQueryStringBuilder.fields()
                        .compute(
                            translatedField,
                            (k, v) -> (v == null) ? requestedFieldNameOrPattern.getValue() : v * requestedFieldNameOrPattern.getValue()
                        );
                    fieldNameVisitor.accept(translatedField);
                }
            }
            if (simpleQueryStringBuilder.fields().isEmpty()) {
                // A SimpleQueryStringBuilder with empty fields() will eventually produce a SimpleQueryString
                // Lucene query that accesses all the fields, including disallowed ones.
                // Instead, the behavior we're after here is that a query that accesses only disallowed fields
                // mustn't match any docs.
                return new MatchNoneQueryBuilder().boost(simpleQueryStringBuilder.boost()).queryName(simpleQueryStringBuilder.queryName());
            }
            return simpleQueryStringBuilder.analyzer(query.analyzer())
                .defaultOperator(query.defaultOperator())
                .minimumShouldMatch(query.minimumShouldMatch())
                .flags(query.flags())
                .type(query.type())
                .quoteFieldSuffix(query.quoteFieldSuffix())
                .analyzeWildcard(query.analyzeWildcard())
                .autoGenerateSynonymsPhraseQuery(query.autoGenerateSynonymsPhraseQuery())
                .fuzzyTranspositions(query.fuzzyTranspositions())
                .fuzzyMaxExpansions(query.fuzzyMaxExpansions())
                .fuzzyPrefixLength(query.fuzzyPrefixLength())
                .boost(query.boost())
                .queryName(query.queryName());
        } else {
            throw new IllegalArgumentException("Query type [" + queryBuilder.getName() + "] is not currently supported in this context");
        }
    }

    /**
     * Adds the {@param fieldSortBuilders} to the {@param searchSourceBuilder}, translating the field names,
     * form query level to index level, see {@link #translate}.
     * The optional {@param visitor} can be used to collect all the translated field names.
     */
    public void translateFieldSortBuilders(
        List<FieldSortBuilder> fieldSortBuilders,
        SearchSourceBuilder searchSourceBuilder,
        @Nullable Consumer<String> visitor
    ) {
        final Consumer<String> fieldNameVisitor = visitor != null ? visitor : ignored -> {};
        fieldSortBuilders.forEach(fieldSortBuilder -> {
            if (fieldSortBuilder.getNestedSort() != null) {
                throw new IllegalArgumentException("nested sorting is not currently supported in this context");
            }
            if (FieldSortBuilder.DOC_FIELD_NAME.equals(fieldSortBuilder.getFieldName())) {
                searchSourceBuilder.sort(fieldSortBuilder);
            } else {
                final String translatedFieldName = translate(fieldSortBuilder.getFieldName(), true);
                fieldNameVisitor.accept(translatedFieldName);
                if (translatedFieldName.equals(fieldSortBuilder.getFieldName())) {
                    searchSourceBuilder.sort(fieldSortBuilder);
                } else {
                    final FieldSortBuilder translatedFieldSortBuilder = new FieldSortBuilder(translatedFieldName).order(
                        fieldSortBuilder.order()
                    )
                        .missing(fieldSortBuilder.missing())
                        .unmappedType(fieldSortBuilder.unmappedType())
                        .setFormat(fieldSortBuilder.getFormat());

                    if (fieldSortBuilder.sortMode() != null) {
                        translatedFieldSortBuilder.sortMode(fieldSortBuilder.sortMode());
                    }
                    if (fieldSortBuilder.getNestedSort() != null) {
                        translatedFieldSortBuilder.setNestedSort(fieldSortBuilder.getNestedSort());
                    }
                    if (fieldSortBuilder.getNumericType() != null) {
                        translatedFieldSortBuilder.setNumericType(fieldSortBuilder.getNumericType());
                    }
                    searchSourceBuilder.sort(translatedFieldSortBuilder);
                }
            }
        });
    }

    /**
     * Translate the query level field name to index level field names.
     * It throws an exception if the field name is not explicitly allowed.
     */
    public String translate(String queryFieldName) {
        return translate(queryFieldName, false);
    }

    /**
     * Translate the query level field name to index level field names.
     * It throws an exception if the field name is not explicitly allowed.
     */
    private String translate(String queryFieldName, boolean inSortContext) {
        // protected for testing
        if (Regex.isSimpleMatchPattern(queryFieldName)) {
            throw new IllegalArgumentException("Field name pattern [" + queryFieldName + "] is not allowed for querying or aggregation");
        }
        for (FieldNameTranslator translator : fieldNameTranslators) {
            if (translator.isQueryFieldSupported(queryFieldName)) {
                if (inSortContext && translator.isSortSupported() == false) {
                    throw new IllegalArgumentException(Strings.format("sorting is not supported for field [%s]", queryFieldName));
                }
                return translator.translate(queryFieldName);
            }
        }
        throw new IllegalArgumentException("Field [" + queryFieldName + "] is not allowed for querying or aggregation");
    }

    /**
     * Translates a query level field name pattern to the matching index level field names.
     * The result can be the empty set, if the pattern doesn't match any of the allowed index level field names.
     */
    public Set<String> translatePattern(String fieldNameOrPattern) {
        Set<String> indexFieldNames = new HashSet<>();
        for (FieldNameTranslator translator : fieldNameTranslators) {
            if (translator.isQueryFieldSupported(fieldNameOrPattern)) {
                indexFieldNames.add(translator.translate(fieldNameOrPattern));
            }
        }
        // It's OK to "translate" to the empty set the concrete disallowed or unknown field names.
        // For eg, the SimpleQueryString query type is lenient in the sense that it ignores unknown fields and field name patterns,
        // so this preprocessing can ignore them too.
        return indexFieldNames;
    }

    public boolean isQueryFieldSupported(String fieldName) {
        return fieldNameTranslators.stream().anyMatch(t -> t.isQueryFieldSupported(fieldName));
    }

    public boolean isIndexFieldSupported(String fieldName) {
        return fieldNameTranslators.stream().anyMatch(t -> t.isIndexFieldSupported(fieldName));
    }

    private interface FieldNameTranslator {
        String translate(String fieldName);

        boolean isQueryFieldSupported(String fieldName);

        boolean isIndexFieldSupported(String fieldName);

        boolean isSortSupported();
    }

    private static SimpleFieldNameTranslator idemFieldNameTranslator(String fieldName) {
        return new SimpleFieldNameTranslator(fieldName, fieldName);
    }

    private static SimpleFieldNameTranslator idemFieldNameTranslator(String fieldName, boolean isSortSupported) {
        return new SimpleFieldNameTranslator(fieldName, fieldName, isSortSupported);
    }

    private static class SimpleFieldNameTranslator implements FieldNameTranslator {
        private final String indexFieldName;
        private final String queryFieldName;
        private final boolean isSortSupported;

        SimpleFieldNameTranslator(String indexFieldName, String queryFieldName, boolean isSortSupported) {
            this.indexFieldName = indexFieldName;
            this.queryFieldName = queryFieldName;
            this.isSortSupported = isSortSupported;
        }

        SimpleFieldNameTranslator(String indexFieldName, String queryFieldName) {
            this(indexFieldName, queryFieldName, true);
        }

        @Override
        public boolean isQueryFieldSupported(String fieldNameOrPattern) {
            if (Regex.isSimpleMatchPattern(fieldNameOrPattern)) {
                return Regex.simpleMatch(fieldNameOrPattern, queryFieldName);
            } else {
                return queryFieldName.equals(fieldNameOrPattern);
            }
        }

        @Override
        public boolean isIndexFieldSupported(String fieldName) {
            return fieldName.equals(indexFieldName);
        }

        @Override
        public String translate(String fieldNameOrPattern) {
            return indexFieldName;
        }

        @Override
        public boolean isSortSupported() {
            return isSortSupported;
        }
    }

    private static class FlattenedFieldNameTranslator implements FieldNameTranslator {
        private final String indexFieldName;
        private final String queryFieldName;

        FlattenedFieldNameTranslator(String indexFieldName, String queryFieldName) {
            this.indexFieldName = indexFieldName;
            this.queryFieldName = queryFieldName;
        }

        @Override
        public boolean isQueryFieldSupported(String fieldNameOrPattern) {
            if (Regex.isSimpleMatchPattern(fieldNameOrPattern)) {
                // It is not possible to translate a pattern for subfields of a flattened field
                // (because there's no list of subfields of the flattened field).
                // But the pattern can still match the flattened field itself.
                return Regex.simpleMatch(fieldNameOrPattern, queryFieldName);
            } else {
                return fieldNameOrPattern.equals(queryFieldName) || fieldNameOrPattern.startsWith(queryFieldName + ".");
            }
        }

        @Override
        public boolean isIndexFieldSupported(String fieldName) {
            return fieldName.equals(indexFieldName) || fieldName.startsWith(indexFieldName + ".");
        }

        @Override
        public String translate(String fieldNameOrPattern) {
            if (Regex.isSimpleMatchPattern(fieldNameOrPattern) || fieldNameOrPattern.equals(queryFieldName)) {
                // the pattern can only refer to the flattened field itself, not to its subfields
                return indexFieldName;
            } else {
                assert fieldNameOrPattern.startsWith(queryFieldName + ".");
                return indexFieldName + fieldNameOrPattern.substring(queryFieldName.length());
            }
        }

        @Override
        public boolean isSortSupported() {
            return true;
        }
    }
}
