/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.eql.execution.assembler;

import org.elasticsearch.common.Strings;
import org.elasticsearch.common.collect.Tuple;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

import static java.util.stream.Collectors.toList;
import static org.elasticsearch.common.logging.LoggerMessageFormat.format;

class SeriesUtils {

    private SeriesUtils() {}

    private enum SpecItem {
        NAME, EVENT_STREAM, RESULTS
    }

    static class SeriesSpec {
        String name;
        int lineNumber;
        boolean hasKeys;
        List<Map<Integer, Tuple<String, String>>> eventsPerCriterion = new ArrayList<>();
        List<List<String>> matches = new ArrayList<>();
        Map<Integer, String> allEvents = new HashMap<>();


        Object[] toArray() {
            return new Object[] { name, lineNumber, this };
        }
    }

    static Iterable<Object[]> readSpec(String url) throws Exception {
        Map<String, Integer> testNames = new LinkedHashMap<>();

        List<SeriesSpec> specs = new ArrayList<>();
        SpecItem readerState = SpecItem.NAME;
        SeriesSpec spec = new SeriesSpec();

        try (
             InputStreamReader in = new InputStreamReader(SeriesUtils.class.getResourceAsStream(url), StandardCharsets.UTF_8);
             BufferedReader reader = new BufferedReader(in)
        ) {
            int lineNumber = 0;
            
            String line;
            while ((line = reader.readLine()) != null) {
                lineNumber++;
                line = line.trim();
                // skip comments
                if (line.isEmpty() || line.startsWith("//")) {
                    continue;
                }

                switch (readerState) {
                    case NAME:
                        spec.name = line;
                        spec.lineNumber = lineNumber;
                        Integer previousLine = testNames.put(line, lineNumber);
                        
                        if (previousLine != null) {
                            throw new IllegalArgumentException(format(null,
                                    "Duplicate test name '{}' at line [{}] (previously seen at line [{}])",
                                    line, lineNumber, previousLine));
                        }
                        readerState = SpecItem.EVENT_STREAM;
                        break;
                    case EVENT_STREAM:
                        if (line.endsWith(";")) {
                            line = line.substring(0, line.length() - 1);
                            readerState = SpecItem.RESULTS;
                            if (Strings.hasText(line) == false) {
                                break;
                            }
                        }

                        String[] events = Strings.tokenizeToStringArray(line, " ");

                        Map<Integer, Tuple<String, String>> eventsMap = new TreeMap<>();

                        for (String event : events) {
                            String key = null;
                            int i = event.indexOf("|");
                            // check if keys are being used
                            if (i > 0) {
                                key = event.substring(0, i);
                                event = event.substring(i + 1);
                                // validate
                                if (spec.allEvents.isEmpty() == false && spec.hasKeys == false) {
                                    throw new IllegalArgumentException(format(null,
                                            "Cannot have a mixture of key [{}] and non-key [{}] events at line [{}]",
                                            event, spec.allEvents.values().iterator().next(), lineNumber));
                                    
                                }
                                spec.hasKeys = true;
                            } else {
                                if (spec.hasKeys) {
                                    throw new IllegalArgumentException(format(null,
                                            "Cannot have a mixture of key [{}] and non-key [{}] events at line [{}]",
                                            event, spec.allEvents.values().iterator().next(), lineNumber));
                                }
                            }
                            // find number
                            int id = event.chars()
                                    .filter(Character::isDigit)
                                    .map(Character::getNumericValue)
                                    .reduce(0, (l, r) -> l * 10 + r);
                            String old = spec.allEvents.put(id, event);
                            if (old != null) {
                                throw new IllegalArgumentException(format(null,
                                        "Detected colision for id [{}] between [{}] and [{}] at line [{}]",
                                        id, old, event, lineNumber));
                            }
                            eventsMap.put(id, new Tuple<>(key, event));
                        }
                        
                        spec.eventsPerCriterion.add(eventsMap);

                        break;
                    case RESULTS:
                        if (line.endsWith(";")) {
                            line = line.substring(0, line.length() - 1);
                            readerState = SpecItem.NAME;
                            specs.add(spec);
                        }

                        if (Strings.hasText(line)) {
                            spec.matches.add(Arrays.asList(Strings.tokenizeToStringArray(line, " ")));
                        }

                        if (readerState != SpecItem.RESULTS) {
                            spec = new SeriesSpec();
                        }
                        break;
                    default:
                        throw new IllegalStateException("Invalid parser state " + readerState);
                }
            }
        }

        if (readerState != SpecItem.NAME) {
            throw new IllegalStateException(format(null, "Read test [{}] with an incomplete body at [{}]", spec.name, url));
        }
        
        return specs.stream().map(SeriesSpec::toArray).collect(toList());
    }
}