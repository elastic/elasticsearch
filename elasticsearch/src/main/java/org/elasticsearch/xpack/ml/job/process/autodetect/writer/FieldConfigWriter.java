/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.ml.job.process.autodetect.writer;

import static org.elasticsearch.xpack.ml.job.process.autodetect.writer.WriterConstants.EQUALS;

import java.io.IOException;
import java.io.OutputStreamWriter;
import java.util.List;
import java.util.Objects;
import java.util.Set;

import org.apache.logging.log4j.Logger;

import org.elasticsearch.common.Strings;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.xpack.ml.job.config.AnalysisConfig;
import org.elasticsearch.xpack.ml.job.config.Detector;
import org.elasticsearch.xpack.ml.job.config.DefaultDetectorDescription;
import org.elasticsearch.xpack.ml.job.config.DetectionRule;
import org.elasticsearch.xpack.ml.job.config.ListDocument;
import org.elasticsearch.xpack.ml.utils.MlStrings;

public class FieldConfigWriter {
    private static final String DETECTOR_PREFIX = "detector.";
    private static final String DETECTOR_CLAUSE_SUFFIX = ".clause";
    private static final String DETECTOR_RULES_SUFFIX = ".rules";
    private static final String INFLUENCER_PREFIX = "influencer.";
    private static final String CATEGORIZATION_FIELD_OPTION = " categorizationfield=";
    private static final String CATEGORIZATION_FILTER_PREFIX = "categorizationfilter.";
    private static final String LIST_PREFIX = "list.";

    // Note: for the Engine API summarycountfield is currently passed as a
    // command line option to autodetect rather than in the field config file

    private static final char NEW_LINE = '\n';

    private final AnalysisConfig config;
    private final Set<ListDocument> lists;
    private final OutputStreamWriter writer;
    private final Logger logger;

    public FieldConfigWriter(AnalysisConfig config, Set<ListDocument> lists,
            OutputStreamWriter writer, Logger logger) {
        this.config = Objects.requireNonNull(config);
        this.lists = Objects.requireNonNull(lists);
        this.writer = Objects.requireNonNull(writer);
        this.logger = Objects.requireNonNull(logger);
    }

    /**
     * Write the Ml autodetect field options to the outputIndex stream.
     */
    public void write() throws IOException {
        StringBuilder contents = new StringBuilder();

        writeDetectors(contents);
        writeLists(contents);
        writeAsEnumeratedSettings(CATEGORIZATION_FILTER_PREFIX, config.getCategorizationFilters(),
                contents, true);

        // As values are written as entire settings rather than part of a
        // clause no quoting is needed
        writeAsEnumeratedSettings(INFLUENCER_PREFIX, config.getInfluencers(), contents, false);

        logger.debug("FieldConfig:\n" + contents.toString());

        writer.write(contents.toString());
    }

    private void writeDetectors(StringBuilder contents) throws IOException {
        int counter = 0;
        for (Detector detector : config.getDetectors()) {
            int detectorId = counter++;
            writeDetectorClause(detectorId, detector, contents);
            writeDetectorRules(detectorId, detector, contents);
        }
    }

    private void writeDetectorClause(int detectorId, Detector detector, StringBuilder contents) {
        contents.append(DETECTOR_PREFIX).append(detectorId)
        .append(DETECTOR_CLAUSE_SUFFIX).append(EQUALS);

        DefaultDetectorDescription.appendOn(detector, contents);

        if (Strings.isNullOrEmpty(config.getCategorizationFieldName()) == false) {
            contents.append(CATEGORIZATION_FIELD_OPTION)
            .append(quoteField(config.getCategorizationFieldName()));
        }

        contents.append(NEW_LINE);
    }

    private void writeDetectorRules(int detectorId, Detector detector, StringBuilder contents) throws IOException {
        List<DetectionRule> rules = detector.getDetectorRules();
        if (rules == null || rules.isEmpty()) {
            return;
        }

        contents.append(DETECTOR_PREFIX).append(detectorId)
        .append(DETECTOR_RULES_SUFFIX).append(EQUALS);

        contents.append('[');
        boolean first = true;
        for (DetectionRule rule : detector.getDetectorRules()) {
            if (first) {
                first = false;
            } else {
                contents.append(',');
            }
            contents.append(rule.toXContent(XContentFactory.jsonBuilder(), ToXContent.EMPTY_PARAMS).string());
        }
        contents.append(']');

        contents.append(NEW_LINE);
    }

    private void writeLists(StringBuilder buffer) throws IOException {
        for (ListDocument list : lists) {

            StringBuilder listAsJson = new StringBuilder();
            listAsJson.append('[');
            boolean first = true;
            for (String item : list.getItems()) {
                if (first) {
                    first = false;
                } else {
                    listAsJson.append(',');
                }
                listAsJson.append('"');
                listAsJson.append(item);
                listAsJson.append('"');
            }
            listAsJson.append(']');
            buffer.append(LIST_PREFIX).append(list.getId()).append(EQUALS).append(listAsJson)
            .append(NEW_LINE);
        }
    }

    private static void writeAsEnumeratedSettings(String settingName, List<String> values, StringBuilder buffer, boolean quote) {
        if (values == null) {
            return;
        }

        int counter = 0;
        for (String value : values) {
            buffer.append(settingName).append(counter++).append(EQUALS)
            .append(quote ? quoteField(value) : value).append(NEW_LINE);
        }
    }

    private static String quoteField(String field) {
        return MlStrings.doubleQuoteIfNotAlphaNumeric(field);
    }
}
