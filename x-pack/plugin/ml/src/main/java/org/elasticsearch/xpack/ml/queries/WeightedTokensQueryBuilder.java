/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.ml.queries;

import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.BooleanClause;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.BoostQuery;
import org.apache.lucene.search.MatchNoDocsQuery;
import org.apache.lucene.search.Query;
import org.elasticsearch.TransportVersion;
import org.elasticsearch.TransportVersions;
import org.elasticsearch.common.ParsingException;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.index.mapper.MappedFieldType;
import org.elasticsearch.index.query.AbstractQueryBuilder;
import org.elasticsearch.index.query.SearchExecutionContext;
import org.elasticsearch.xcontent.ParseField;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xpack.core.ml.inference.results.TextExpansionResults.WeightedToken;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

public class WeightedTokensQueryBuilder extends AbstractQueryBuilder<WeightedTokensQueryBuilder> {
  public static final String NAME = "weighted_tokens";

  public static final ParseField TOKENS_FIELD = new ParseField("tokens");
  public static final ParseField RATIO_THRESHOLD_FIELD = new ParseField("ratio_threshold");
  public static final ParseField WEIGHT_THRESHOLD_FIELD = new ParseField("weight_threshold");
  private final String fieldName;
  private final List<WeightedToken> tokens;
  private final int ratioThreshold;
  private final float weightThreshold;

  public WeightedTokensQueryBuilder(String fieldName,
                                    List<WeightedToken> tokens) {
    this(fieldName, tokens, -1, 1f);
  }

  public WeightedTokensQueryBuilder(String fieldName,
                                    List<WeightedToken> tokens,
                                    int ratioThreshold, float weightThreshold) {
    this.fieldName = Objects.requireNonNull(fieldName, "[" + NAME + "] requires a fieldName");
    this.tokens = Objects.requireNonNull(tokens, "[" + NAME + "] requires tokens");
    this.ratioThreshold = ratioThreshold;
    this.weightThreshold = weightThreshold;

    if (weightThreshold < 0 || weightThreshold > 1) {
      throw new IllegalArgumentException("[" + NAME + "] requires the "
              + WEIGHT_THRESHOLD_FIELD.getPreferredName() + " to be between 0 and 1, got " + weightThreshold);
    }
  }

  public WeightedTokensQueryBuilder(StreamInput in) throws IOException {
    super(in);
    this.fieldName = in.readString();
    this.tokens = in.readCollectionAsList(WeightedToken::new);
    this.ratioThreshold = in.readInt();
    this.weightThreshold = in.readFloat();
  }

  public String getFieldName() {
    return fieldName;
  }

  /**
   * Returns the frequency ratio threshold to apply on the query.
   * Tokens whose frequency is more than ratio_threshold times the average frequency of all tokens in the specified
   * field are considered outliers and may be subject to removal from the query.
   */
  public int getRatioThreshold() {
    return ratioThreshold;
  }

  /**
   * Returns the weight threshold to apply on the query.
   * Tokens whose weight is more than (weightThreshold * best_weight) of the highest weight in the query are not
   * considered outliers, even if their frequency exceeds the specified ratio_threshold.
   * This threshold ensures that important tokens, as indicated by their weight, are retained in the query.
   */
  public float getWeightThreshold() {
    return weightThreshold;
  }

  @Override
  protected void doWriteTo(StreamOutput out) throws IOException {
    out.writeString(fieldName);
    out.writeCollection(tokens);
    out.writeInt(ratioThreshold);
    out.writeFloat(weightThreshold);
  }

  @Override
  protected void doXContent(XContentBuilder builder, Params params) throws IOException {
    builder.startObject(NAME);
    builder.startObject(fieldName);
    builder.field(TOKENS_FIELD.getPreferredName(), tokens);
    if (ratioThreshold > 0) {
      builder.field(RATIO_THRESHOLD_FIELD.getPreferredName(), ratioThreshold);
      builder.field(WEIGHT_THRESHOLD_FIELD.getPreferredName(), weightThreshold);
    }
    boostAndQueryNameToXContent(builder);
    builder.endObject();
    builder.endObject();
  }

  private float getAverageTokenFreq(IndexReader reader) throws IOException {
    int numUniqueTokens = 0;
    for (var leaf : reader.getContext().leaves()) {
      var terms = leaf.reader().terms(fieldName);
      if (terms != null) {
        numUniqueTokens = (int) Math.max(terms.size(), numUniqueTokens);
      }
    }
    if (numUniqueTokens == 0) {
      return 0;
    }
    return (float) reader.getSumDocFreq(fieldName) / reader.getDocCount(fieldName) / numUniqueTokens;
  }

  /**
   * Returns true if the token should be queried based on the {@code ratioThreshold} and {@code weightThreshold}
   * set on the query.
   */
  private boolean shouldKeepToken(IndexReader reader,
                                  WeightedToken token,
                                  int fieldDocCount,
                                  float averageTokenFreq,
                                  float bestWeight) throws IOException {
    int docFreq = reader.docFreq(new Term(fieldName, token.token()));
    if (docFreq == 0) {
      return false;
    }
    float tokenFreq = (float) docFreq / fieldDocCount;
    return tokenFreq < ratioThreshold * averageTokenFreq
            || token.weight() > weightThreshold * bestWeight;
  }

  @Override
  protected Query doToQuery(SearchExecutionContext context) throws IOException {
    final MappedFieldType mapper = context.getFieldType(fieldName);
    if (mapper == null) {
      return new MatchNoDocsQuery("The \"" + getName() + "\" query is against a field that does not exist");
    }
    var qb = new BooleanQuery.Builder();
    int fieldDocCount = context.getIndexReader().getDocCount(fieldName);
    float bestWeight = 0f;
    for (var t : tokens) {
      bestWeight = Math.max(t.weight(), bestWeight);
    }
    float averageTokenFreq = getAverageTokenFreq(context.getIndexReader());
    if (averageTokenFreq == 0) {
      return new MatchNoDocsQuery("The \"" + getName() + "\" query is against an empty field");
    }
    for (var token : tokens) {
      if (shouldKeepToken(context.getIndexReader(), token, fieldDocCount, averageTokenFreq, bestWeight)) {
        qb.add(new BoostQuery(mapper.termQuery(token.token(), context), token.weight()), BooleanClause.Occur.SHOULD);
      }
    }
    return qb.build();
  }

  @Override
  protected boolean doEquals(WeightedTokensQueryBuilder other) {
    return Float.compare(weightThreshold, other.weightThreshold) == 0 &&
            ratioThreshold == other.ratioThreshold &&
            tokens.equals(other.tokens);
  }

  @Override
  protected int doHashCode() {
    return Objects.hash(tokens, ratioThreshold, weightThreshold);
  }

  @Override
  public String getWriteableName() {
    return NAME;
  }

  @Override
  public TransportVersion getMinimalSupportedVersion() {
    return TransportVersions.WEIGHTED_TOKENS_QUERY_ADDED;
  }

  private static float parseWeight(String token, Object weight) {
    if (weight instanceof Number asNumber) {
      return asNumber.floatValue();
    }
    if (weight instanceof String asString) {
      return Float.valueOf(asString);
    }
    throw new IllegalArgumentException("Illegal weight for token: ["
            + token + "], expected floating point got " + weight.getClass().getSimpleName());
  }

  public static WeightedTokensQueryBuilder fromXContent(XContentParser parser) throws IOException {
    String currentFieldName = null;
    String fieldName = null;
    List<WeightedToken> tokens = new ArrayList<>();
    int ratioThreshold = 0;
    float weightThreshold = 1f;
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
          } else if (RATIO_THRESHOLD_FIELD.match(currentFieldName, parser.getDeprecationHandler())) {
            ratioThreshold = parser.intValue();
          } else if (WEIGHT_THRESHOLD_FIELD.match(currentFieldName, parser.getDeprecationHandler())) {
            weightThreshold = parser.floatValue();
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
            throw new ParsingException(
                    parser.getTokenLocation(),
                    "[" + NAME + "] query does not support [" + currentFieldName + "]"
            );
          }
        }
      } else {
        throw new IllegalArgumentException("invalid query");
      }
    }

    if (fieldName == null) {
      throw new ParsingException(parser.getTokenLocation(), "No fieldname specified for query");
    }

    var qb = new WeightedTokensQueryBuilder(fieldName, tokens, ratioThreshold, weightThreshold);
    qb.queryName(queryName);
    qb.boost(boost);
    return qb;
  }
}