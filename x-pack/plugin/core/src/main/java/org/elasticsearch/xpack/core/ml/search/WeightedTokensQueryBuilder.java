/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.core.ml.search;

import org.apache.lucene.search.MatchNoDocsQuery;
import org.apache.lucene.search.Query;
import org.elasticsearch.ElasticsearchParseException;
import org.elasticsearch.TransportVersion;
import org.elasticsearch.TransportVersions;
import org.elasticsearch.common.ParsingException;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.logging.DeprecationCategory;
import org.elasticsearch.common.logging.DeprecationLogger;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.index.mapper.MappedFieldType;
import org.elasticsearch.index.mapper.vectors.TokenPruningConfig;
import org.elasticsearch.index.query.AbstractQueryBuilder;
import org.elasticsearch.index.query.SearchExecutionContext;
import org.elasticsearch.inference.WeightedToken;
import org.elasticsearch.inference.WeightedTokensUtils;
import org.elasticsearch.xcontent.ParseField;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentParser;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.Set;

/**
 * @deprecated Replaced by sparse_vector query
 */
@Deprecated
public class WeightedTokensQueryBuilder extends AbstractQueryBuilder<WeightedTokensQueryBuilder> {

    public static final String NAME = "weighted_tokens";

    public static final ParseField TOKENS_FIELD = new ParseField("tokens");
    public static final ParseField PRUNING_CONFIG = new ParseField("pruning_config");
    private final String fieldName;
    private final List<WeightedToken> tokens;
    @Nullable
    private final TokenPruningConfig tokenPruningConfig;

    private static final Set<String> ALLOWED_FIELD_TYPES = Set.of("sparse_vector", "rank_features");

    private static final DeprecationLogger deprecationLogger = DeprecationLogger.getLogger(ParseField.class);
    public static final String WEIGHTED_TOKENS_DEPRECATION_MESSAGE = NAME
        + " is deprecated and will be removed. Use sparse_vector instead.";

    public WeightedTokensQueryBuilder(String fieldName, List<WeightedToken> tokens) {
        this(fieldName, tokens, null);
    }

    public WeightedTokensQueryBuilder(String fieldName, List<WeightedToken> tokens, @Nullable TokenPruningConfig tokenPruningConfig) {
        this.fieldName = Objects.requireNonNull(fieldName, "[" + NAME + "] requires a fieldName");
        this.tokens = Objects.requireNonNull(tokens, "[" + NAME + "] requires tokens");
        if (tokens.isEmpty()) {
            throw new IllegalArgumentException("[" + NAME + "] requires at least one token");
        }
        this.tokenPruningConfig = tokenPruningConfig;
    }

    public WeightedTokensQueryBuilder(StreamInput in) throws IOException {
        super(in);
        this.fieldName = in.readString();
        this.tokens = in.readCollectionAsList(WeightedToken::new);
        this.tokenPruningConfig = in.readOptionalWriteable(TokenPruningConfig::new);
    }

    public String getFieldName() {
        return fieldName;
    }

    public List<WeightedToken> getTokens() {
        return tokens;
    }

    @Override
    protected void doWriteTo(StreamOutput out) throws IOException {
        out.writeString(fieldName);
        out.writeCollection(tokens);
        out.writeOptionalWriteable(tokenPruningConfig);
    }

    @Override
    protected void doXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject(NAME);
        builder.startObject(fieldName);
        builder.startObject(TOKENS_FIELD.getPreferredName());
        for (var token : tokens) {
            token.toXContent(builder, params);
        }
        builder.endObject();
        if (tokenPruningConfig != null) {
            builder.field(PRUNING_CONFIG.getPreferredName(), tokenPruningConfig);
        }
        boostAndQueryNameToXContent(builder);
        builder.endObject();
        builder.endObject();
    }

    @Override
    protected Query doToQuery(SearchExecutionContext context) throws IOException {
        final MappedFieldType ft = context.getFieldType(fieldName);
        if (ft == null) {
            return new MatchNoDocsQuery("The \"" + getName() + "\" query is against a field that does not exist");
        }

        final String fieldTypeName = ft.typeName();
        if (ALLOWED_FIELD_TYPES.contains(fieldTypeName) == false) {
            throw new ElasticsearchParseException(
                "["
                    + fieldTypeName
                    + "]"
                    + " is not an appropriate field type for this query. "
                    + "Allowed field types are ["
                    + String.join(", ", ALLOWED_FIELD_TYPES)
                    + "]."
            );
        }

        return (this.tokenPruningConfig == null)
            ? WeightedTokensUtils.queryBuilderWithAllTokens(fieldName, tokens, ft, context)
            : WeightedTokensUtils.queryBuilderWithPrunedTokens(fieldName, tokenPruningConfig, tokens, ft, context);
    }

    @Override
    protected boolean doEquals(WeightedTokensQueryBuilder other) {
        return Objects.equals(fieldName, other.fieldName)
            && Objects.equals(tokenPruningConfig, other.tokenPruningConfig)
            && tokens.equals(other.tokens);
    }

    @Override
    protected int doHashCode() {
        return Objects.hash(fieldName, tokens, tokenPruningConfig);
    }

    @Override
    public String getWriteableName() {
        return NAME;
    }

    @Override
    public TransportVersion getMinimalSupportedVersion() {
        return TransportVersions.V_8_13_0;
    }

    private static float parseWeight(String token, Object weight) {
        if (weight instanceof Number asNumber) {
            return asNumber.floatValue();
        }
        if (weight instanceof String asString) {
            return Float.parseFloat(asString);
        }
        throw new ElasticsearchParseException(
            "Illegal weight for token: [" + token + "], expected floating point got " + weight.getClass().getSimpleName()
        );
    }

    public static WeightedTokensQueryBuilder fromXContent(XContentParser parser) throws IOException {

        deprecationLogger.critical(DeprecationCategory.API, NAME, WEIGHTED_TOKENS_DEPRECATION_MESSAGE);

        String currentFieldName = null;
        String fieldName = null;
        List<WeightedToken> tokens = new ArrayList<>();
        TokenPruningConfig tokenPruningConfig = null;
        float boost = AbstractQueryBuilder.DEFAULT_BOOST;
        String queryName = null;
        XContentParser.Token token;
        while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
            if (token == XContentParser.Token.FIELD_NAME) {
                currentFieldName = parser.currentName();
            } else if (token == XContentParser.Token.START_OBJECT) {
                throwParsingExceptionOnMultipleFields(NAME, parser.getTokenLocation(), fieldName, currentFieldName);
                fieldName = currentFieldName;
                while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
                    if (token == XContentParser.Token.FIELD_NAME) {
                        currentFieldName = parser.currentName();
                    } else if (PRUNING_CONFIG.match(currentFieldName, parser.getDeprecationHandler())) {
                        if (token != XContentParser.Token.START_OBJECT) {
                            throw new ParsingException(
                                parser.getTokenLocation(),
                                "[" + PRUNING_CONFIG.getPreferredName() + "] should be an object"
                            );
                        }
                        tokenPruningConfig = TokenPruningConfig.fromXContent(parser);
                    } else if (TOKENS_FIELD.match(currentFieldName, parser.getDeprecationHandler())) {
                        var tokensMap = parser.map();
                        for (var e : tokensMap.entrySet()) {
                            tokens.add(new WeightedToken(e.getKey(), parseWeight(e.getKey(), e.getValue())));
                        }
                    } else if (AbstractQueryBuilder.BOOST_FIELD.match(currentFieldName, parser.getDeprecationHandler())) {
                        boost = parser.floatValue();
                    } else if (AbstractQueryBuilder.NAME_FIELD.match(currentFieldName, parser.getDeprecationHandler())) {
                        queryName = parser.text();
                    } else {
                        throw new ParsingException(parser.getTokenLocation(), "unknown field [" + currentFieldName + "]");
                    }
                }
            } else {
                throw new IllegalArgumentException("invalid query");
            }
        }

        if (fieldName == null) {
            throw new ParsingException(parser.getTokenLocation(), "No fieldname specified for query");
        }

        var qb = new WeightedTokensQueryBuilder(fieldName, tokens, tokenPruningConfig);
        qb.queryName(queryName);
        qb.boost(boost);
        return qb;
    }
}
