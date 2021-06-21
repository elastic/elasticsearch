/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 * This Java port of CLD3 was derived from Google's CLD3 project at https://github.com/google/cld3
 */

package org.elasticsearch.xpack.core.ml.inference.trainedmodel.langident;

import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.common.xcontent.ParseField;
import org.elasticsearch.common.xcontent.DeprecationHandler;
import org.elasticsearch.common.xcontent.NamedXContentRegistry;
import org.elasticsearch.common.xcontent.ObjectParser;
import org.elasticsearch.common.xcontent.ToXContentObject;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.common.xcontent.XContentType;

import java.io.IOException;
import java.net.URL;
import java.util.ArrayList;
import java.util.List;

/**
 * These language examples are gathered from Google's CLD3 testing code.
 */
public final class LanguageExamples {

    public LanguageExamples() {}

    public  List<LanguageExampleEntry> getLanguageExamples() throws IOException {
        String path = "/org/elasticsearch/xpack/core/ml/inference/language_examples.json";
        URL resource = getClass().getResource(path);
        if (resource == null) {
            throw new ElasticsearchException("Could not find resource stored at [" + path + "]");
        }
        try(XContentParser parser =
                XContentType.JSON.xContent().createParser(
                    NamedXContentRegistry.EMPTY,
                    DeprecationHandler.THROW_UNSUPPORTED_OPERATION,
                    getClass().getResourceAsStream(path))) {
            List<LanguageExampleEntry> entries = new ArrayList<>();
            while(parser.nextToken() != XContentParser.Token.END_ARRAY) {
                entries.add(LanguageExampleEntry.PARSER.apply(parser, null));
            }
            return entries;
        }
    }

    public static class LanguageExampleEntry implements ToXContentObject {
        private static final ParseField LANGUAGE = new ParseField("language");
        private static final ParseField PREDICTED_LANGUAGE = new ParseField("predicted_language");
        private static final ParseField PROBABILITY = new ParseField("probability");
        private static final ParseField TEXT = new ParseField("text");

        public static ObjectParser<LanguageExampleEntry, Void> PARSER = new ObjectParser<>(
            "language_example_entry",
            true,
            LanguageExampleEntry::new);

        static {
            PARSER.declareString(LanguageExampleEntry::setLanguage, LANGUAGE);
            PARSER.declareString(LanguageExampleEntry::setPredictedLanguage, PREDICTED_LANGUAGE);
            PARSER.declareDouble(LanguageExampleEntry::setProbability, PROBABILITY);
            PARSER.declareString(LanguageExampleEntry::setText, TEXT);
        }
        // The true language
        String language;
        // The language predicted by CLD3
        String predictedLanguage;
        // The probability of the prediction
        double probability;
        // The raw text on which the prediction is based
        String text;

        private LanguageExampleEntry() {
        }

        private void setLanguage(String language) {
            this.language = language;
        }

        private void setPredictedLanguage(String predictedLanguage) {
            this.predictedLanguage = predictedLanguage;
        }

        private void setProbability(double probability) {
            this.probability = probability;
        }

        private void setText(String text) {
            this.text = text;
        }

        public String getLanguage() {
            return language;
        }

        public String getPredictedLanguage() {
            return predictedLanguage;
        }

        public double getProbability() {
            return probability;
        }

        public String getText() {
            return text;
        }

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            builder.startObject();
            builder.field(LANGUAGE.getPreferredName(), language);
            builder.field(PREDICTED_LANGUAGE.getPreferredName(), predictedLanguage);
            builder.field(PROBABILITY.getPreferredName(), probability);
            builder.field(TEXT.getPreferredName(), text);
            builder.endObject();
            return builder;
        }
    }
}
