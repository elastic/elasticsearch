/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.elasticsearch.client.ml.inference.preprocessing;

import org.elasticsearch.common.CheckedFunction;
import org.elasticsearch.common.ParseField;
import org.elasticsearch.common.xcontent.ConstructingObjectParser;
import org.elasticsearch.common.xcontent.ObjectParser;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentParser;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;

/**
 * This is a pre-processor that embeds text into a numerical vector.
 *
 * It calculates a set of features based on script type, ngram hashes, and most common script values.
 *
 * The features are then concatenated with specific quantization scales and weights into a vector of length 80.
 *
 * This is a fork and a port of: https://github.com/google/cld3/blob/06f695f1c8ee530104416aab5dcf2d6a1414a56a/src/embedding_network.cc
 */
public class CustomWordEmbedding implements PreProcessor {

    public static final String NAME = "custom_word_embedding";
    static final ParseField FIELD = new ParseField("field");
    static final ParseField DEST_FIELD = new ParseField("dest_field");
    static final ParseField EMBEDDING_WEIGHTS = new ParseField("embedding_weights");
    static final ParseField EMBEDDING_QUANT_SCALES = new ParseField("embedding_quant_scales");

    public static final ConstructingObjectParser<CustomWordEmbedding, Void> PARSER = new ConstructingObjectParser<>(
        NAME,
        true,
        a -> new CustomWordEmbedding((short[][])a[0], (byte[][])a[1], (String)a[2], (String)a[3]));
    static {
        PARSER.declareField(ConstructingObjectParser.constructorArg(),
            (p, c) -> {
                List<List<Short>> listOfListOfShorts = parseArrays(EMBEDDING_QUANT_SCALES.getPreferredName(),
                    XContentParser::shortValue,
                    p);
                short[][] primitiveShorts = new short[listOfListOfShorts.size()][];
                int i = 0;
                for (List<Short> shorts : listOfListOfShorts) {
                    short[] innerShorts = new short[shorts.size()];
                    for (int j = 0; j < shorts.size(); j++) {
                        innerShorts[j] = shorts.get(j);
                    }
                    primitiveShorts[i++] = innerShorts;
                }
                return primitiveShorts;
            },
            EMBEDDING_QUANT_SCALES,
            ObjectParser.ValueType.VALUE_ARRAY);
        PARSER.declareField(ConstructingObjectParser.constructorArg(),
            (p, c) -> {
                List<byte[]> values = new ArrayList<>();
                while(p.nextToken() != XContentParser.Token.END_ARRAY) {
                    values.add(p.binaryValue());
                }
                byte[][] primitiveBytes = new byte[values.size()][];
                int i = 0;
                for (byte[] bytes : values) {
                    primitiveBytes[i++] = bytes;
                }
                return primitiveBytes;
            },
            EMBEDDING_WEIGHTS,
            ObjectParser.ValueType.VALUE_ARRAY);
        PARSER.declareString(ConstructingObjectParser.constructorArg(), FIELD);
        PARSER.declareString(ConstructingObjectParser.constructorArg(), DEST_FIELD);
    }

    private static <T> List<List<T>> parseArrays(String fieldName,
                                                 CheckedFunction<XContentParser, T, IOException> fromParser,
                                                 XContentParser p) throws IOException {
        if (p.currentToken() != XContentParser.Token.START_ARRAY) {
            throw new IllegalArgumentException("unexpected token [" + p.currentToken() + "] for [" + fieldName + "]");
        }
        List<List<T>> values = new ArrayList<>();
        while(p.nextToken() != XContentParser.Token.END_ARRAY) {
            if (p.currentToken() != XContentParser.Token.START_ARRAY) {
                throw new IllegalArgumentException("unexpected token [" + p.currentToken() + "] for [" + fieldName + "]");
            }
            List<T> innerList = new ArrayList<>();
            while(p.nextToken() != XContentParser.Token.END_ARRAY) {
                if(p.currentToken().isValue() == false) {
                    throw new IllegalStateException("expected non-null value but got [" + p.currentToken() + "] " +
                        "for [" + fieldName + "]");
                }
                innerList.add(fromParser.apply(p));
            }
            values.add(innerList);
        }
        return values;
    }

    public static CustomWordEmbedding fromXContent(XContentParser parser) {
        return PARSER.apply(parser, null);
    }

    private final short[][] embeddingsQuantScales;
    private final byte[][] embeddingsWeights;
    private final String fieldName;
    private final String destField;

    CustomWordEmbedding(short[][] embeddingsQuantScales, byte[][] embeddingsWeights, String fieldName, String destField) {
        this.embeddingsQuantScales = embeddingsQuantScales;
        this.embeddingsWeights = embeddingsWeights;
        this.fieldName = fieldName;
        this.destField = destField;
    }

    @Override
    public String getName() {
        return NAME;
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, ToXContent.Params params) throws IOException {
        builder.startObject();
        builder.field(FIELD.getPreferredName(), fieldName);
        builder.field(DEST_FIELD.getPreferredName(), destField);
        builder.field(EMBEDDING_QUANT_SCALES.getPreferredName(), embeddingsQuantScales);
        builder.field(EMBEDDING_WEIGHTS.getPreferredName(), embeddingsWeights);
        builder.endObject();
        return builder;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        CustomWordEmbedding that = (CustomWordEmbedding) o;
        return Objects.equals(fieldName, that.fieldName)
            && Objects.equals(destField, that.destField)
            && Arrays.deepEquals(embeddingsWeights, that.embeddingsWeights)
            && Arrays.deepEquals(embeddingsQuantScales, that.embeddingsQuantScales);
    }

    @Override
    public int hashCode() {
        return Objects.hash(fieldName, destField, Arrays.deepHashCode(embeddingsQuantScales), Arrays.deepHashCode(embeddingsWeights));
    }

}
