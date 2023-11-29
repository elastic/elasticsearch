/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.ml.queries;

import org.apache.lucene.index.Term;
import org.apache.lucene.search.*;
import org.elasticsearch.TransportVersion;
import org.elasticsearch.TransportVersions;
import org.elasticsearch.common.ParsingException;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.index.mapper.MappedFieldType;
import org.elasticsearch.index.query.AbstractQueryBuilder;
import org.elasticsearch.index.query.MatchNoneQueryBuilder;
import org.elasticsearch.index.query.SearchExecutionContext;
import org.elasticsearch.xcontent.ParseField;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xpack.core.ml.inference.results.TextExpansionResults;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

public class WeightedTermsQueryBuilder extends AbstractQueryBuilder<WeightedTermsQueryBuilder> {
  public static final String NAME = "weighted_terms";

  public static final ParseField TERMS_FIELD = new ParseField("terms");
  public static final ParseField CUTOFF_FREQUENCY_RATIO_FIELD = new ParseField("cutoff_frequency_ratio");
  public static final ParseField CUTOFF_BEST_WEIGHT_FIELD = new ParseField("cutoff_best_weight");
  public static final ParseField QUERY_STRATEGY_FIELD = new ParseField("query_strategy");


  private static final int DEFAULT_CUTOFF_FREQUENCY_RATIO = 5;
  private static final float DEFAULT_CUTOFF_BEST_WEIGHT = 0.4f;
  private static final QueryStrategy DEFAULT_QUERY_STRATEGY = QueryStrategy.REMOVE_HIGH_FREQUENCY_TERMS;

  enum QueryStrategy {
    REMOVE_HIGH_FREQUENCY_TERMS,
    REMOVE_LOW_FREQUENCY_TERMS,
    ONLY_SCORE_LOW_FREQUENCY_TERMS
  }

  private final String fieldName;
  private final List<TextExpansionResults.WeightedToken> tokens;
  private final int cutoffFrequencyRatio;
  private final float cutoffBestWeight;
  private final QueryStrategy queryStrategy;

  public WeightedTermsQueryBuilder(String fieldName,
                                   List<TextExpansionResults.WeightedToken> tokens) {
    this(fieldName, tokens, 5, 0.4f, QueryStrategy.REMOVE_HIGH_FREQUENCY_TERMS);
  }

  public WeightedTermsQueryBuilder(String fieldName,
                                   List<TextExpansionResults.WeightedToken> tokens,
                                   int cutoffFrequencyRatio, float cutoffBestWeight,
                                   QueryStrategy queryStrategy) {
    this.fieldName = fieldName;
    this.tokens = tokens;
    this.cutoffFrequencyRatio = cutoffFrequencyRatio;
    this.cutoffBestWeight = cutoffBestWeight;
    this.queryStrategy = queryStrategy;
  }

  public WeightedTermsQueryBuilder(StreamInput in) throws IOException {
    super(in);
    this.fieldName = in.readString();
    this.tokens = in.readCollectionAsList(TextExpansionResults.WeightedToken::new);
    this.cutoffFrequencyRatio = in.readInt();
    this.cutoffBestWeight = in.readFloat();
    this.queryStrategy = in.readEnum(QueryStrategy.class);
  }

  @Override
  protected void doWriteTo(StreamOutput out) throws IOException {
    out.writeString(fieldName);
    out.writeCollection(tokens);
    out.writeInt(cutoffFrequencyRatio);
    out.writeFloat(cutoffBestWeight);
    out.writeEnum(queryStrategy);
  }

  @Override
  protected void doXContent(XContentBuilder builder, Params params) throws IOException {
    // TODO
  }

  @Override
  protected Query doToQuery(SearchExecutionContext context) throws IOException {
    final MappedFieldType mapper = context.getFieldType(fieldName);
    if (mapper == null) {
      return new MatchNoDocsQuery("The \"" + getName() + "\" query is against a field that does not exist");
    }
    var qb = new BooleanQuery.Builder();
    var lowTerms = new BooleanQuery.Builder();
    int maxDocs = context.getIndexReader().maxDoc();
    float bestWeight = tokens.get(0).weight();
    int vocabSize = 0;
    for (var leaf : context.getIndexReader().getContext().leaves()) {
      var terms = leaf.reader().terms(fieldName);
      if (terms != null) {
        vocabSize = (int) Math.max(terms.size(), vocabSize);
      }
    }
    float averageTokenFreq = (float) context.getIndexReader().getSumDocFreq(fieldName) /
            context.getIndexReader().maxDoc() / vocabSize;
    for (var token : tokens) {
      var term = new Term(fieldName, token.token());
      float termFreq = (float) context.getIndexReader().docFreq(term) / maxDocs;
      if (termFreq > cutoffFrequencyRatio * averageTokenFreq
              && token.weight() < cutoffBestWeight * bestWeight) {
        qb.add(new BoostQuery(mapper.termQuery(token.token(), context), token.weight()), BooleanClause.Occur.SHOULD);
      } else {
        lowTerms.add(new BoostQuery(mapper.termQuery(token.token(), context), token.weight()), BooleanClause.Occur.SHOULD);
      }
    }
    switch (queryStrategy) {
      case REMOVE_HIGH_FREQUENCY_TERMS:
        return lowTerms.build();
      case REMOVE_LOW_FREQUENCY_TERMS:
        return qb.build();
      case ONLY_SCORE_LOW_FREQUENCY_TERMS:
        return qb.add(lowTerms.build(), BooleanClause.Occur.MUST).build();
      default:
        throw new IllegalArgumentException(("Unknown query strategy: " + queryStrategy.name()));
    }
  }

  @Override
  protected boolean doEquals(WeightedTermsQueryBuilder other) {
    return Float.compare(cutoffBestWeight, other.cutoffBestWeight) == 0 &&
            cutoffFrequencyRatio == other.cutoffFrequencyRatio &&
            queryStrategy == other.queryStrategy &&
            tokens.equals(other.tokens);
  }

  @Override
  protected int doHashCode() {
    return Objects.hash(tokens, cutoffBestWeight, cutoffFrequencyRatio, queryStrategy);
  }

  @Override
  public String getWriteableName() {
    return NAME;
  }

  @Override
  public TransportVersion getMinimalSupportedVersion() {
    // TODO: upgrade the transport version
    return TransportVersions.MINIMUM_CCS_VERSION;
  }

  public static WeightedTermsQueryBuilder fromXContent(XContentParser parser) throws IOException {
    String currentFieldName = null;
    String fieldName = null;
    List<TextExpansionResults.WeightedToken> tokens = new ArrayList<>();
    int cutoffFrequencyRatio = DEFAULT_CUTOFF_FREQUENCY_RATIO;
    float cutoffBestWeight = DEFAULT_CUTOFF_BEST_WEIGHT;
    QueryStrategy queryStrategy = DEFAULT_QUERY_STRATEGY;
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
          } else if (CUTOFF_BEST_WEIGHT_FIELD.match(currentFieldName, parser.getDeprecationHandler())) {
            cutoffBestWeight = parser.floatValue();
          } else if (CUTOFF_FREQUENCY_RATIO_FIELD.match(currentFieldName, parser.getDeprecationHandler())) {
            cutoffFrequencyRatio = parser.intValue();
          } else if (QUERY_STRATEGY_FIELD.match(currentFieldName, parser.getDeprecationHandler())) {
            queryStrategy = QueryStrategy.valueOf(parser.text().toUpperCase());
          } else if (TERMS_FIELD.match(currentFieldName, parser.getDeprecationHandler())) {
            var tokensMap = parser.map();
            for (var e : tokensMap.entrySet()) {
              tokens.add(new TextExpansionResults.WeightedToken(e.getKey(), ((Double) e.getValue()).floatValue()));
            }
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

    return new WeightedTermsQueryBuilder(fieldName, tokens, cutoffFrequencyRatio, cutoffBestWeight, queryStrategy);
  }
}