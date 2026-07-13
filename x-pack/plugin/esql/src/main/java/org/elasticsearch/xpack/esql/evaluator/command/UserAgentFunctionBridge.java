/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.evaluator.command;

import org.elasticsearch.useragent.api.Details;
import org.elasticsearch.useragent.api.UserAgentParsedInfo;
import org.elasticsearch.useragent.api.UserAgentParser;

import java.util.SequencedCollection;
import java.util.SequencedMap;
import java.util.function.BiConsumer;

import static org.elasticsearch.useragent.api.UserAgentParsedInfo.DEVICE_NAME;
import static org.elasticsearch.useragent.api.UserAgentParsedInfo.DEVICE_TYPE;
import static org.elasticsearch.useragent.api.UserAgentParsedInfo.NAME;
import static org.elasticsearch.useragent.api.UserAgentParsedInfo.OS_FULL;
import static org.elasticsearch.useragent.api.UserAgentParsedInfo.OS_NAME;
import static org.elasticsearch.useragent.api.UserAgentParsedInfo.OS_VERSION;
import static org.elasticsearch.useragent.api.UserAgentParsedInfo.VERSION;
import static org.elasticsearch.xpack.esql.evaluator.command.CompoundOutputEvaluator.NOOP_STRING_COLLECTOR;
import static org.elasticsearch.xpack.esql.evaluator.command.CompoundOutputEvaluator.stringValueCollector;

/**
 * Bridge for the USER_AGENT command that parses user-agent strings into structured fields.
 */
public final class UserAgentFunctionBridge {

    public static SequencedMap<String, Class<?>> getAllOutputFields() {
        return UserAgentParsedInfo.getUserAgentInfoFields();
    }

    public static final class UserAgentCollectorImpl extends CompoundOutputEvaluator.OutputFieldsCollector {
        private final UserAgentParser parser;
        private final boolean extractDeviceType;

        private final BiConsumer<CompoundOutputEvaluator.RowOutput, String> name;
        private final BiConsumer<CompoundOutputEvaluator.RowOutput, String> version;
        private final BiConsumer<CompoundOutputEvaluator.RowOutput, String> osName;
        private final BiConsumer<CompoundOutputEvaluator.RowOutput, String> osVersion;
        private final BiConsumer<CompoundOutputEvaluator.RowOutput, String> osFull;
        private final BiConsumer<CompoundOutputEvaluator.RowOutput, String> deviceName;
        private final BiConsumer<CompoundOutputEvaluator.RowOutput, String> deviceType;

        public UserAgentCollectorImpl(SequencedCollection<String> outputFields, UserAgentParser parser, boolean extractDeviceType) {
            super(outputFields.size());
            this.parser = parser;
            this.extractDeviceType = extractDeviceType;

            BiConsumer<CompoundOutputEvaluator.RowOutput, String> name = NOOP_STRING_COLLECTOR;
            BiConsumer<CompoundOutputEvaluator.RowOutput, String> version = NOOP_STRING_COLLECTOR;
            BiConsumer<CompoundOutputEvaluator.RowOutput, String> osName = NOOP_STRING_COLLECTOR;
            BiConsumer<CompoundOutputEvaluator.RowOutput, String> osVersion = NOOP_STRING_COLLECTOR;
            BiConsumer<CompoundOutputEvaluator.RowOutput, String> osFull = NOOP_STRING_COLLECTOR;
            BiConsumer<CompoundOutputEvaluator.RowOutput, String> deviceName = NOOP_STRING_COLLECTOR;
            BiConsumer<CompoundOutputEvaluator.RowOutput, String> deviceType = NOOP_STRING_COLLECTOR;

            int index = 0;
            for (String outputField : outputFields) {
                switch (outputField) {
                    case NAME -> name = stringValueCollector(index);
                    case VERSION -> version = stringValueCollector(index);
                    case OS_NAME -> osName = stringValueCollector(index);
                    case OS_VERSION -> osVersion = stringValueCollector(index);
                    case OS_FULL -> osFull = stringValueCollector(index);
                    case DEVICE_NAME -> deviceName = stringValueCollector(index);
                    case DEVICE_TYPE -> deviceType = stringValueCollector(index);
                    default -> {
                        // unknown field — corresponding block will be filled with nulls
                    }
                }
                index++;
            }

            this.name = name;
            this.version = version;
            this.osName = osName;
            this.osVersion = osVersion;
            this.osFull = osFull;
            this.deviceName = deviceName;
            this.deviceType = deviceType;
        }

        @Override
        protected void evaluate(String input) {
            Details details = parser.parseUserAgentInfo(input, extractDeviceType);
            name.accept(rowOutput, details.name());
            version.accept(rowOutput, details.version());
            if (details.os() != null) {
                osName.accept(rowOutput, details.os().name());
                osVersion.accept(rowOutput, details.os().version());
            }
            osFull.accept(rowOutput, details.osFull());
            if (details.device() != null) {
                deviceName.accept(rowOutput, details.device().name());
            }
            boolean parsed = details.name() != null || details.os() != null || details.device() != null;
            deviceType.accept(rowOutput, parsed ? details.deviceType() : null);
        }
    }
}
