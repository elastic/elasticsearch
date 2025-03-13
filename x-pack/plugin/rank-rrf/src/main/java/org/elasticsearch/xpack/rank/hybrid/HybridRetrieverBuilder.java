/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.rank.hybrid;

import org.elasticsearch.common.collect.ImmutableOpenMap;
import org.elasticsearch.common.xcontent.LoggingDeprecationHandler;
import org.elasticsearch.common.xcontent.support.XContentMapValues;
import org.elasticsearch.index.query.MatchQueryBuilder;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.search.rank.RankBuilder;
import org.elasticsearch.search.retriever.CompoundRetrieverBuilder;
import org.elasticsearch.search.retriever.RetrieverBuilder;
import org.elasticsearch.search.retriever.RetrieverBuilderWrapper;
import org.elasticsearch.search.retriever.RetrieverParserContext;
import org.elasticsearch.search.retriever.StandardRetrieverBuilder;
import org.elasticsearch.xcontent.ConstructingObjectParser;
import org.elasticsearch.xcontent.NamedXContentRegistry;
import org.elasticsearch.xcontent.ParseField;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentParseException;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xcontent.support.MapXContentParser;
import org.elasticsearch.xpack.inference.rank.textsimilarity.TextSimilarityRankRetrieverBuilder;
import org.elasticsearch.xpack.rank.linear.LinearRetrieverBuilder;
import org.elasticsearch.xpack.rank.linear.MinMaxScoreNormalizer;
import org.elasticsearch.xpack.rank.linear.ScoreNormalizer;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

import static org.elasticsearch.search.retriever.CompoundRetrieverBuilder.RANK_WINDOW_SIZE_FIELD;
import static org.elasticsearch.xcontent.ConstructingObjectParser.constructorArg;
import static org.elasticsearch.xcontent.ConstructingObjectParser.optionalConstructorArg;
import static org.elasticsearch.xpack.rank.hybrid.QuerySettings.TYPE_FIELD;

// TODO:
// - Retriever name support

public class HybridRetrieverBuilder extends RetrieverBuilderWrapper<HybridRetrieverBuilder> {
    public static final String NAME = "hybrid";
    public static final ParseField FIELDS_FIELD = new ParseField("fields");
    public static final ParseField QUERY_FIELD = new ParseField("query");
    public static final ParseField RERANK_FIELD = new ParseField("rerank");
    public static final ParseField RERANK_FIELD_FIELD = new ParseField("rerank_field");
    public static final ParseField RERANK_INFERENCE_ID_FIELD = new ParseField("rerank_inference_id");
    public static final ParseField QUERY_SETTINGS_FIELD = new ParseField("query_settings");

    private final List<String> fields;
    private final String query;
    private final Boolean rerank;
    private final String rerankField;
    private final String rerankInferenceId;
    private final Map<String, List<QuerySettings>> querySettingsMap;
    private final int rankWindowSize;

    @SuppressWarnings("unchecked")
    private static final ConstructingObjectParser<HybridRetrieverBuilder, RetrieverParserContext> PARSER = new ConstructingObjectParser<>(
        NAME,
        false,
        (args, context) -> {
            List<String> fields = (List<String>) args[0];
            String query = (String) args[1];
            Boolean rerank = (Boolean) args[2];
            String rerankField = (String) args[3];
            String rerankInferenceId = (String) args[4];
            int rankWindowSize = args[5] == null ? RankBuilder.DEFAULT_RANK_WINDOW_SIZE : (int) args[5];
            Map<String, List<QuerySettings>> querySettingsMap = (Map<String, List<QuerySettings>>) args[6];

            return new HybridRetrieverBuilder(fields, query, rerank, rerankField, rerankInferenceId, querySettingsMap, rankWindowSize);
        }
    );

    private static final NamedXContentRegistry NAMED_X_CONTENT_REGISTRY;

    static {
        List<NamedXContentRegistry.Entry> xContentRegistryEntries = List.of(
            new NamedXContentRegistry.Entry(
                QuerySettings.class,
                new ParseField(MatchQuerySettings.QUERY_TYPE.getQueryName()),
                MatchQuerySettings::fromXContent
            ),
            new NamedXContentRegistry.Entry(
                QuerySettings.class,
                new ParseField(MatchPhraseQuerySettings.QUERY_TYPE.getQueryName()),
                MatchPhraseQuerySettings::fromXContent
            )
        );

        NAMED_X_CONTENT_REGISTRY = new NamedXContentRegistry(xContentRegistryEntries);

        PARSER.declareStringArray(constructorArg(), FIELDS_FIELD);
        PARSER.declareString(constructorArg(), QUERY_FIELD);
        PARSER.declareBoolean(optionalConstructorArg(), RERANK_FIELD);
        PARSER.declareString(optionalConstructorArg(), RERANK_FIELD_FIELD);
        PARSER.declareString(optionalConstructorArg(), RERANK_INFERENCE_ID_FIELD);
        PARSER.declareInt(optionalConstructorArg(), RANK_WINDOW_SIZE_FIELD);
        PARSER.declareObject(optionalConstructorArg(), (p, c) -> {
            Map<String, List<QuerySettings>> querySettingsMap = new HashMap<>();

            Map<String, Object> unparsedMap = p.map();
            for (var entry : unparsedMap.entrySet()) {
                String field = entry.getKey();
                Object value = entry.getValue();

                if (value instanceof List<?> list) {
                    List<QuerySettings> querySettings = querySettingsMap.computeIfAbsent(field, f -> new ArrayList<>(list.size()));
                    for (Object listValue : list) {
                        if (listValue instanceof Map<?, ?> map) {
                            querySettings.add(parseQuerySettings(map));
                        } else {
                            throw new IllegalArgumentException(
                                "Query settings for field [" + field + "] must be an object or list of objects"
                            );
                        }
                    }
                } else if (value instanceof Map<?, ?> map) {
                    List<QuerySettings> querySettings = querySettingsMap.computeIfAbsent(field, f -> new ArrayList<>());
                    querySettings.add(parseQuerySettings(map));
                } else {
                    throw new IllegalArgumentException("Query settings for field [" + field + "] must be an object or list of objects");
                }
            }

            return querySettingsMap;
        }, QUERY_SETTINGS_FIELD);
        RetrieverBuilder.declareBaseParserFields(PARSER);
    }

    public HybridRetrieverBuilder(
        List<String> fields,
        String query,
        Boolean rerank,
        String rerankField,
        String rerankInferenceId,
        Map<String, List<QuerySettings>> querySettingsMap,
        int rankWindowSize
    ) {
        this(
            fields == null ? List.of() : List.copyOf(fields),
            query,
            rerank,
            rerankField,
            rerankInferenceId,
            copyQuerySettingsMap(querySettingsMap),
            rankWindowSize,
            generateRetrieverBuilder(fields, query, rerank, rerankField, rerankInferenceId, querySettingsMap, rankWindowSize)
        );
    }

    private HybridRetrieverBuilder(
        List<String> fields,
        String query,
        Boolean rerank,
        String rerankField,
        String rerankInferenceId,
        Map<String, List<QuerySettings>> querySettingsMap,
        int rankWindowSize,
        RetrieverBuilder retrieverBuilder
    ) {
        super(retrieverBuilder);
        this.fields = fields;
        this.query = query;
        this.rerank = rerank;
        this.rerankField = rerankField;
        this.rerankInferenceId = rerankInferenceId;
        this.querySettingsMap = querySettingsMap;
        this.rankWindowSize = rankWindowSize;
    }

    @Override
    protected HybridRetrieverBuilder clone(RetrieverBuilder sub) {
        return new HybridRetrieverBuilder(fields, query, rerank, rerankField, rerankInferenceId, querySettingsMap, rankWindowSize, sub);
    }

    @Override
    public String getName() {
        return NAME;
    }

    @Override
    protected void doToXContent(XContentBuilder builder, Params params) throws IOException {
        builder.field(FIELDS_FIELD.getPreferredName(), fields);
        builder.field(QUERY_FIELD.getPreferredName(), query);
        if (rerank != null) {
            builder.field(RERANK_FIELD.getPreferredName(), rerank);
        }
        if (rerankField != null) {
            builder.field(RERANK_FIELD_FIELD.getPreferredName(), rerankField);
        }
        if (rerankInferenceId != null) {
            builder.field(RERANK_INFERENCE_ID_FIELD.getPreferredName(), rerankInferenceId);
        }
        builder.field(RANK_WINDOW_SIZE_FIELD.getPreferredName(), rankWindowSize);
    }

    @Override
    protected boolean doEquals(Object o) {
        // TODO: Check rankWindowSize? It should be checked by the wrapped retriever.
        HybridRetrieverBuilder that = (HybridRetrieverBuilder) o;
        return Objects.equals(fields, that.fields)
            && Objects.equals(query, that.query)
            && Objects.equals(rerank, that.rerank)
            && Objects.equals(rerankField, that.rerankField)
            && Objects.equals(rerankInferenceId, that.rerankInferenceId)
            && super.doEquals(o);
    }

    @Override
    protected int doHashCode() {
        return Objects.hash(fields, query, rerank, rerankField, rerankInferenceId, super.doHashCode());
    }

    public static HybridRetrieverBuilder fromXContent(XContentParser parser, RetrieverParserContext context) throws IOException {
        return PARSER.apply(parser, context);
    }

    private static Map<String, List<QuerySettings>> copyQuerySettingsMap(Map<String, List<QuerySettings>> querySettingsMap) {
        if (querySettingsMap == null) {
            return Map.of();
        }

        ImmutableOpenMap.Builder<String, List<QuerySettings>> copyBuilder = new ImmutableOpenMap.Builder<>(querySettingsMap.size());
        for (var entry : querySettingsMap.entrySet()) {
            String field = entry.getKey();
            List<QuerySettings> querySettings = entry.getValue();

            copyBuilder.put(field, querySettings != null ? List.copyOf(querySettings) : List.of());
        }

        return copyBuilder.build();
    }

    private static RetrieverBuilder generateRetrieverBuilder(
        List<String> fields,
        String query,
        Boolean rerank,
        String rerankField,
        String rerankInferenceId,
        Map<String, List<QuerySettings>> querySettingsMap,
        int rankWindowSize
    ) {
        FieldsAndsWeights fieldsAndsWeights = generateFieldsAndWeights(fields);

        LinearRetrieverBuilder linearRetrieverBuilder = new LinearRetrieverBuilder(
            generateInnerRetrievers(fieldsAndsWeights.fields(), query, querySettingsMap),
            rankWindowSize,
            fieldsAndsWeights.weights(),
            generateScoreNormalizers(fields)
        );

        RetrieverBuilder rootRetriever = linearRetrieverBuilder;
        if (rerank != null && rerank) {
            if (rerankField == null) {
                throw new IllegalArgumentException("[" + RERANK_FIELD_FIELD.getPreferredName() + "] is required when reranking is enabled");
            }

            rootRetriever = new TextSimilarityRankRetrieverBuilder(
                linearRetrieverBuilder,
                rerankInferenceId,
                query,
                rerankField,
                rankWindowSize,
                false
            );
        }

        return rootRetriever;
    }

    private static List<CompoundRetrieverBuilder.RetrieverSource> generateInnerRetrievers(
        List<String> fields,
        String query,
        Map<String, List<QuerySettings>> querySettingsMap
    ) {
        if (fields == null) {
            return List.of();
        }

        List<CompoundRetrieverBuilder.RetrieverSource> innerRetrievers = new ArrayList<>();
        for (String field : fields) {
            List<QueryBuilder> fieldQueryBuilders = new ArrayList<>();
            List<QuerySettings> fieldQuerySettings = querySettingsMap != null ? querySettingsMap.get(field) : null;
            if (fieldQuerySettings == null || fieldQuerySettings.isEmpty()) {
                // Default to match query
                fieldQueryBuilders.add(new MatchQueryBuilder(field, query));
            } else {
                for (QuerySettings querySettings : fieldQuerySettings) {
                    fieldQueryBuilders.add(querySettings.constructQueryBuilder(field, query));
                }
            }

            for (QueryBuilder queryBuilder : fieldQueryBuilders) {
                innerRetrievers.add(new CompoundRetrieverBuilder.RetrieverSource(new StandardRetrieverBuilder(queryBuilder), null));
            }
        }

        return innerRetrievers;
    }

    private static FieldsAndsWeights generateFieldsAndWeights(List<String> fields) {
        if (fields == null) {
            return new FieldsAndsWeights(List.of(), new float[0]);
        }

        int fieldCount = fields.size();
        List<String> parsedFields = new ArrayList<>(fieldCount);
        float[] parsedWeights = new float[fieldCount];
        for (int i = 0; i < fieldCount; i++) {
            String[] fieldSplit = fields.get(i).split("\\^");

            float weight = 1.0f;
            if (fieldSplit.length > 2) {
                throw new IllegalArgumentException("Invalid field name [" + fields.get(i) + "]");
            } else if (fieldSplit.length == 2) {
                weight = Float.parseFloat(fieldSplit[1]);
            }

            parsedFields.add(fieldSplit[0]);
            parsedWeights[i] = weight;
        }

        return new FieldsAndsWeights(Collections.unmodifiableList(parsedFields), parsedWeights);
    }

    private static ScoreNormalizer[] generateScoreNormalizers(List<String> fields) {
        if (fields == null) {
            return new ScoreNormalizer[0];
        }

        ScoreNormalizer[] scoreNormalizers = new ScoreNormalizer[fields.size()];
        Arrays.fill(scoreNormalizers, new MinMaxScoreNormalizer(0));
        return scoreNormalizers;
    }

    private record FieldsAndsWeights(List<String> fields, float[] weights) {}

    // TODO: Probably a better way to do this, but this is quick & dirty for POC purposes
    private static QuerySettings parseQuerySettings(Map<?, ?> map) {
        Map<String, Object> querySettingsMap = XContentMapValues.nodeMapValue(map, "query settings");

        Object typeObject = querySettingsMap.get(TYPE_FIELD.getPreferredName());
        if (typeObject == null) {
            throw new IllegalArgumentException("[" + TYPE_FIELD.getPreferredName() + "] must be provided in query settings");
        } else if (typeObject instanceof String == false) {
            throw new IllegalArgumentException("[" + TYPE_FIELD.getPreferredName() + "] must have a string value");
        }

        String typeString = (String) typeObject;
        MapXContentParser mapXContentParser = new MapXContentParser(
            NAMED_X_CONTENT_REGISTRY,
            LoggingDeprecationHandler.INSTANCE,
            querySettingsMap,
            null
        );

        try (mapXContentParser) {
            return mapXContentParser.namedObject(QuerySettings.class, typeString, null);
        } catch (IOException e) {
            throw new XContentParseException(mapXContentParser.getTokenLocation(), "Failed to parse query settings");
        }
    }
}
