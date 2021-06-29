/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.core.ml.inference.preprocessing;


import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;

import org.apache.lucene.util.RamUsageEstimator;
import org.elasticsearch.common.xcontent.ParseField;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.xcontent.ObjectParser;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.xpack.core.ml.utils.ExceptionsHelper;
import org.elasticsearch.xpack.core.ml.utils.NamedXContentObjectHelper;

/**
 * Multi-PreProcessor for chaining together multiple processors
 */
public class Multi implements LenientlyParsedPreProcessor, StrictlyParsedPreProcessor {

    public static final long SHALLOW_SIZE = RamUsageEstimator.shallowSizeOfInstance(Multi.class);
    public static final ParseField NAME = new ParseField("multi_encoding");
    public static final ParseField PROCESSORS = new ParseField("processors");
    public static final ParseField CUSTOM = new ParseField("custom");

    private static final ObjectParser<Multi.Builder, PreProcessorParseContext> STRICT_PARSER = createParser(false);
    private static final ObjectParser<Multi.Builder, PreProcessorParseContext> LENIENT_PARSER = createParser(true);

    private static ObjectParser<Multi.Builder, PreProcessorParseContext> createParser(boolean lenient) {
        ObjectParser<Multi.Builder, PreProcessorParseContext> parser = new ObjectParser<>(
            NAME.getPreferredName(),
            lenient,
            Multi.Builder::new
        );
        parser.declareNamedObjects(Multi.Builder::setProcessors,
            (p, c, n) -> lenient ?
                p.namedObject(LenientlyParsedPreProcessor.class, n, PreProcessor.PreProcessorParseContext.DEFAULT) :
                p.namedObject(StrictlyParsedPreProcessor.class, n, PreProcessor.PreProcessorParseContext.DEFAULT),
            (multiBuilder) -> multiBuilder.setOrdered(true),
            PROCESSORS);
        parser.declareBoolean(Multi.Builder::setCustom, CUSTOM);
        return parser;
    }

    public static Multi fromXContentStrict(XContentParser parser, PreProcessorParseContext context) {
        return STRICT_PARSER.apply(parser, context == null ?  PreProcessorParseContext.DEFAULT : context).build();
    }

    public static Multi fromXContentLenient(XContentParser parser, PreProcessorParseContext context) {
        return LENIENT_PARSER.apply(parser, context == null ?  PreProcessorParseContext.DEFAULT : context).build();
    }

    private final PreProcessor[] processors;
    private final boolean custom;
    private final Map<String, String> outputFields;
    private final String[] inputFields;

    public Multi(PreProcessor[] processors, Boolean custom) {
        this.processors = ExceptionsHelper.requireNonNull(processors, PROCESSORS);
        if (this.processors.length < 2) {
            throw new IllegalArgumentException("processors must be an array of objects with at least length 2");
        }
        this.custom = custom != null && custom;
        Set<String> consumedOutputFields = new HashSet<>();
        List<String> inputFields = new ArrayList<>(processors[0].inputFields());
        Map<String, String> originatingOutputFields = new LinkedHashMap<>();
        for (String outputField : processors[0].outputFields()) {
            originatingOutputFields.put(outputField, processors[0].getOutputFieldType(outputField));
        }
        for (int i = 1; i < processors.length; i++) {
            final PreProcessor processor = processors[i];
            for (String inputField : processor.inputFields()) {
                if (originatingOutputFields.containsKey(inputField) == false) {
                    inputFields.add(inputField);
                } else {
                    consumedOutputFields.add(inputField);
                }
            }
            for (String outputField : processor.outputFields()) {
                originatingOutputFields.put(outputField, processor.getOutputFieldType(outputField));
            }
        }
        Map<String, String> outputFields = new LinkedHashMap<>();
        for (var outputField : originatingOutputFields.entrySet()) {
            if (consumedOutputFields.contains(outputField.getKey()) == false) {
                outputFields.put(outputField.getKey(), outputField.getValue());
            }
        }
        this.outputFields = outputFields;
        this.inputFields = inputFields.toArray(String[]::new);
        if (this.custom == false && this.inputFields.length > 1) {
            throw new IllegalArgumentException(
                String.format(
                    Locale.ROOT,
                    "[custom] cannot be false as [%s] is unable to accurately determine" +
                        " field reverse encoding for input fields [%s] and output fields %s",
                    NAME.getPreferredName(),
                    Strings.arrayToCommaDelimitedString(this.inputFields),
                    this.outputFields.keySet()
                )
            );
        }
    }

    public Multi(StreamInput in) throws IOException {
        this.processors = in.readNamedWriteableList(PreProcessor.class).toArray(PreProcessor[]::new);
        this.custom = in.readBoolean();
        this.outputFields = in.readOrderedMap(StreamInput::readString, StreamInput::readString);
        this.inputFields = in.readStringArray();
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeNamedWriteableList(Arrays.asList(processors));
        out.writeBoolean(custom);
        out.writeMap(outputFields, StreamOutput::writeString, StreamOutput::writeString);
        out.writeStringArray(inputFields);
    }

    @Override
    public String toString() {
        return Strings.toString(this);
    }

    @Override
    public List<String> inputFields() {
        return Arrays.asList(inputFields);
    }

    @Override
    public List<String> outputFields() {
        return new ArrayList<>(outputFields.keySet());
    }

    @Override
    public void process(Map<String, Object> fields) {
        for (PreProcessor processor : processors) {
            processor.process(fields);
        }
    }

    @Override
    public Map<String, String> reverseLookup() {
        if (inputFields.length > 1) {
            throw new IllegalArgumentException(
                String.format(
                    Locale.ROOT,
                    "[%s] is unable to accurately determine field reverse encoding for input fields [%s] and output fields %s",
                    NAME.getPreferredName(),
                    Strings.arrayToCommaDelimitedString(inputFields),
                    outputFields.keySet()
                )
            );
        }
        return outputFields.keySet().stream().collect(Collectors.toMap(Function.identity(), _unused -> inputFields[0]));
    }

    @Override
    public String getOutputFieldType(String outputField) {
        return outputFields.get(outputField);
    }

    @Override
    public long ramBytesUsed() {
        long size = SHALLOW_SIZE;
        size += RamUsageEstimator.sizeOf(processors);
        size += RamUsageEstimator.sizeOf(inputFields);
        size += RamUsageEstimator.sizeOfMap(outputFields, 0);
        return size;
    }

    @Override
    public String getWriteableName() {
        return NAME.getPreferredName();
    }

    @Override
    public String getName() {
        return NAME.getPreferredName();
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        NamedXContentObjectHelper.writeNamedObjects(builder, params, true, PROCESSORS.getPreferredName(), Arrays.asList(processors));
        builder.field(CUSTOM.getPreferredName(), custom);
        builder.endObject();
        return builder;
    }

    @Override
    public boolean isCustom() {
        return custom;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        Multi multi = (Multi) o;
        return Arrays.equals(multi.processors, processors) && custom == multi.custom;
    }

    @Override
    public int hashCode() {
        return Objects.hash(custom, Arrays.hashCode(processors));
    }

    static class Builder {
        private boolean ordered;
        private List<PreProcessor> processors;
        private boolean custom;

        public Builder setOrdered(boolean ordered) {
            this.ordered = ordered;
            return this;
        }

        public Builder setProcessors(List<PreProcessor> processors) {
            this.processors = processors;
            return this;
        }

        public Builder setCustom(boolean custom) {
            this.custom = custom;
            return this;
        }

        Multi build() {
            if (ordered == false) {
                throw new IllegalArgumentException("processors must be an array of objects");
            }
            if (processors.size() < 2) {
                throw new IllegalArgumentException("processors must be an array of objects with at least length 2");
            }
            return new Multi(processors.toArray(PreProcessor[]::new), custom);
        }
    }

}
