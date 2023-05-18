/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.dlm.dataperiods;

import org.elasticsearch.common.Strings;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.xcontent.ConstructingObjectParser;
import org.elasticsearch.xcontent.ParseField;
import org.elasticsearch.xcontent.ToXContentObject;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentFactory;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xcontent.XContentParserConfiguration;
import org.elasticsearch.xcontent.XContentType;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;

/**
 * This class contains the default data periods configured as a cluster property. It consists of a prioritized valid list
 * of data periods. This means that the list does not contain duplicate or unreachable data period configurations.
 */
public class DefaultDataPeriods implements ToXContentObject {

    static DefaultDataPeriods parseSetting(String input) {
        try (XContentParser parser = XContentFactory.xContent(XContentType.JSON).createParser(XContentParserConfiguration.EMPTY, input)) {
            return DefaultDataPeriods.fromXContent(parser);
        } catch (IOException error) {
            throw new RuntimeException(error);
        }
    }

    private static final ParseField DATA_PERIODS_FIELD = new ParseField("data_periods");

    @SuppressWarnings("unchecked")
    public static final ConstructingObjectParser<DefaultDataPeriods, Void> PARSER = new ConstructingObjectParser<>(
        "default_data_periods",
        false,
        (args, unused) -> new DefaultDataPeriods((List<DataPeriod>) args[0])
    );

    static {
        PARSER.declareObjectArray(ConstructingObjectParser.constructorArg(), (p, c) -> DataPeriod.fromXContent(p), DATA_PERIODS_FIELD);
    }

    public static final Setting<DefaultDataPeriods> DLM_DEFAULT_DATA_PERIOD_SETTING = new Setting<>(
        "indices.dlm.default.data_period",
        "{\"data_periods\":[]}",
        DefaultDataPeriods::parseSetting,
        Setting.Property.Dynamic,
        Setting.Property.NodeScope
    );

    private final List<DataPeriod> dataPeriods;

    public DefaultDataPeriods(List<DataPeriod> input) {
        this.dataPeriods = input.stream()
            .sorted(Comparator.comparing(patterns -> patterns.namePatterns().get(0)))
            .sorted(Comparator.comparingInt(DataPeriod::priority).reversed())
            .toList();
        validateNamePatterns(dataPeriods);
    }

    private void validateNamePatterns(List<DataPeriod> dataPeriods) {
        Map<String, List<String>> unreachableNamePatterns = new HashMap<>();
        for (int patternsToValidateIndex = 1; patternsToValidateIndex < dataPeriods.size(); patternsToValidateIndex++) {
            DataPeriod patternsToValidate = dataPeriods.get(patternsToValidateIndex);
            for (String pattern : patternsToValidate.namePatterns()) {
                String patternPrefixToValidate = pattern.contains("*") ? pattern.substring(0, pattern.length() - 1) : pattern;
                for (int precedingRegexIndex = patternsToValidateIndex - 1; precedingRegexIndex >= 0; precedingRegexIndex--) {
                    for (String regex : dataPeriods.get(precedingRegexIndex).namePatterns()) {
                        if (dataPeriods.get(precedingRegexIndex).match(regex, patternPrefixToValidate)) {
                            unreachableNamePatterns.computeIfAbsent(pattern, ignored -> new ArrayList<>()).add(regex);
                        }
                    }
                }
            }
        }
        if (unreachableNamePatterns.isEmpty() == false) {
            throw new IllegalArgumentException(
                unreachableNamePatterns.entrySet()
                    .stream()
                    .map(
                        entry -> "name pattern '"
                            + entry.getKey()
                            + "' is unreachable because of preceding name patterns: "
                            + entry.getValue()
                    )
                    .collect(Collectors.joining())
            );
        }
    }

    public List<DataPeriod> getDataPeriods() {
        return dataPeriods;
    }

    public static DefaultDataPeriods fromXContent(XContentParser parser) throws IOException {
        return PARSER.parse(parser, null);
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        builder.startArray(DATA_PERIODS_FIELD.getPreferredName());
        for (DataPeriod dataPeriod : dataPeriods) {
            dataPeriod.toXContent(builder, params);
        }
        builder.endArray();
        builder.endObject();
        return builder;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        DefaultDataPeriods that = (DefaultDataPeriods) o;
        return Objects.equals(dataPeriods, that.dataPeriods);
    }

    @Override
    public int hashCode() {
        return Objects.hash(dataPeriods);
    }

    @Override
    public String toString() {
        return Strings.toString(this, true, true);
    }
}
