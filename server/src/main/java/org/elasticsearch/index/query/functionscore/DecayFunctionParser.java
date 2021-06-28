/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.index.query.functionscore;

import org.elasticsearch.common.xcontent.ParseField;
import org.elasticsearch.common.ParsingException;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.plugins.SearchPlugin;
import org.elasticsearch.search.MultiValueMode;
import org.elasticsearch.search.SearchModule;

import java.io.IOException;
import java.util.function.BiFunction;

/**
 * Parser used for all decay functions, one instance each. It parses this kind
 * of input:
 *
 * <pre>
 * <code>
 * {
 *      "fieldname1" : {
 *          "origin" = "someValue",
 *          "scale" = "someValue"
 *      },
 *      "multi_value_mode" : "min"
 * }
 * </code>
 * </pre>
 *
 * "origin" here refers to the reference point and "scale" to the level of
 * uncertainty you have in your origin.
 * <p>
 *
 * For example, you might want to retrieve an event that took place around the
 * 20 May 2010 somewhere near Berlin. You are mainly interested in events that
 * are close to the 20 May 2010 but you are unsure about your guess, maybe it
 * was a week before or after that. Your "origin" for the date field would be
 * "20 May 2010" and your "scale" would be "7d".
 *
 * <p>
 * This class parses the input and creates a scoring function from the
 * parameters origin and scale.
 * <p>
 * To write a new decay scoring function, create a new class that extends
 * {@link DecayFunctionBuilder}, setup a PARSER field with this class, and
 * register them in {@link SearchModule#registerScoreFunctions} or {@link SearchPlugin#getScoreFunctions}
 * See {@link GaussDecayFunctionBuilder#PARSER} for an example.
 */
public final class DecayFunctionParser<DFB extends DecayFunctionBuilder<DFB>> implements ScoreFunctionParser<DFB> {

    public static final ParseField MULTI_VALUE_MODE = new ParseField("multi_value_mode");
    private final BiFunction<String, BytesReference, DFB> createFromBytes;

    /**
     * Create the parser using a method reference to a "create from bytes" constructor for the {@linkplain DecayFunctionBuilder}. We use a
     * method reference here so each use of this class doesn't have to subclass it.
     */
    public DecayFunctionParser(BiFunction<String, BytesReference, DFB> createFromBytes) {
        this.createFromBytes = createFromBytes;
    }

    /**
     * Parses bodies of the kind
     *
     * <pre>
     * <code>
     * {
     *      "fieldname1" : {
     *          "origin" : "someValue",
     *          "scale" : "someValue"
     *      },
     *      "multi_value_mode" : "min"
     * }
     * </code>
     * </pre>
     */
    @Override
    public DFB fromXContent(XContentParser parser) throws IOException, ParsingException {
        String currentFieldName;
        XContentParser.Token token;
        MultiValueMode multiValueMode = DecayFunctionBuilder.DEFAULT_MULTI_VALUE_MODE;
        String fieldName = null;
        BytesReference functionBytes = null;
        while ((token = parser.nextToken()) == XContentParser.Token.FIELD_NAME) {
            currentFieldName = parser.currentName();
            token = parser.nextToken();
            if (token == XContentParser.Token.START_OBJECT) {
                fieldName = currentFieldName;
                XContentBuilder builder = XContentFactory.jsonBuilder();
                builder.copyCurrentStructure(parser);
                functionBytes = BytesReference.bytes(builder);
            } else if (MULTI_VALUE_MODE.match(currentFieldName, parser.getDeprecationHandler())) {
                multiValueMode = MultiValueMode.fromString(parser.text());
            } else {
                throw new ParsingException(parser.getTokenLocation(), "malformed score function score parameters.");
            }
        }
        if (fieldName == null || functionBytes == null) {
            throw new ParsingException(parser.getTokenLocation(), "malformed score function score parameters.");
        }
        DFB functionBuilder = createFromBytes.apply(fieldName, functionBytes);
        functionBuilder.setMultiValueMode(multiValueMode);
        return functionBuilder;
    }
}
