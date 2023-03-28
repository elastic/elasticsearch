/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.ingest.common;

import org.elasticsearch.ingest.AbstractProcessor;
import org.elasticsearch.ingest.ConfigurationUtils;
import org.elasticsearch.ingest.IngestDocument;
import org.elasticsearch.ingest.Processor;

import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.function.Function;
import java.util.stream.Collectors;

import static org.elasticsearch.ingest.ConfigurationUtils.newConfigurationException;

public final class RerouteProcessor extends AbstractProcessor {
    public static final String TYPE = "reroute";

    private static final char[] DISALLOWED_IN_DATASET = new char[] { '\\', '/', '*', '?', '\"', '<', '>', '|', ' ', ',', '#', ':', '-' };
    private static final char[] DISALLOWED_IN_NAMESPACE = new char[] { '\\', '/', '*', '?', '\"', '<', '>', '|', ' ', ',', '#', ':' };
    private static final String DATA_STREAM_PREFIX = "data_stream.";
    private static final String DATA_STREAM_TYPE = DATA_STREAM_PREFIX + "type";
    private static final String DATA_STREAM_DATASET = DATA_STREAM_PREFIX + "dataset";
    private static final DataStreamValueSource DATASET_VALUE_SOURCE = DataStreamValueSource.dataset("{{" + DATA_STREAM_DATASET + "}}");
    private static final String DATA_STREAM_NAMESPACE = DATA_STREAM_PREFIX + "namespace";
    private static final DataStreamValueSource NAMESPACE_VALUE_SOURCE = DataStreamValueSource.namespace("{{" + DATA_STREAM_NAMESPACE + "}}");
    private static final String EVENT_DATASET = "event.dataset";
    private static final int MAX_LENGTH = 100;
    private static final char REPLACEMENT_CHAR = '_';
    private final List<DataStreamValueSource> dataset;
    private final List<DataStreamValueSource> namespace;
    private final String destination;

    RerouteProcessor(
        String tag,
        String description,
        List<DataStreamValueSource> dataset,
        List<DataStreamValueSource> namespace,
        String destination
    ) {
        super(tag, description);
        this.dataset = dataset;
        this.namespace = namespace;
        this.destination = destination;
    }

    private static String sanitizeDataStreamField(String s, char[] disallowedInDataset) {
        if (s == null) {
            return null;
        }
        s = s.toLowerCase(Locale.ROOT);
        s = s.substring(0, Math.min(s.length(), MAX_LENGTH));
        for (char c : disallowedInDataset) {
            s = s.replace(c, REPLACEMENT_CHAR);
        }
        return s;
    }

    @Override
    public IngestDocument execute(IngestDocument ingestDocument) throws Exception {
        if (destination != null) {
            ingestDocument.reroute(destination);
            return ingestDocument;
        }
        final String indexName = ingestDocument.getFieldValue(IngestDocument.Metadata.INDEX.getFieldName(), String.class);
        final String type;
        final String currentDataset;
        final String currentNamespace;
        int indexOfFirstDash = indexName.indexOf('-');
        if (indexOfFirstDash < 0) {
            throw createInvalidDataStreamNameException(indexName);
        }
        int indexOfSecondDash = indexName.indexOf('-', indexOfFirstDash + 1);
        if (indexOfSecondDash < 0) {
            throw createInvalidDataStreamNameException(indexName);
        }
        type = parseDataStreamType(indexName, indexOfFirstDash);
        currentDataset = parseDataStreamDataset(indexName, indexOfFirstDash, indexOfSecondDash);
        currentNamespace = parseDataStreamNamespace(indexName, indexOfSecondDash);

        String dataset = determineDataStreamField(ingestDocument, this.dataset, DATASET_VALUE_SOURCE, currentDataset);
        String namespace = determineDataStreamField(ingestDocument, this.namespace, NAMESPACE_VALUE_SOURCE, currentNamespace);
        if (dataset == null || namespace == null) {
            return ingestDocument;
        }
        String newTarget = type + "-" + dataset + "-" + namespace;
        ingestDocument.reroute(newTarget);
        ingestDocument.setFieldValue(DATA_STREAM_TYPE, type);
        ingestDocument.setFieldValue(DATA_STREAM_DATASET, dataset);
        ingestDocument.setFieldValue(DATA_STREAM_NAMESPACE, namespace);
        if (ingestDocument.hasField(EVENT_DATASET)) {
            // ECS specifies that "event.dataset should have the same value as data_stream.dataset"
            // not eagerly set event.dataset but only if the doc contains it already to ensure it's consistent with data_stream.dataset
            ingestDocument.setFieldValue(EVENT_DATASET, dataset);
        }
        return ingestDocument;
    }

    private static IllegalArgumentException createInvalidDataStreamNameException(String indexName) {
        return new IllegalArgumentException(
            "invalid data stream name: [" + indexName + "]; must follow naming scheme <type>-<dataset>-<namespace>"
        );
    }

    private static String parseDataStreamType(String dataStreamName, int indexOfFirstDash) {
        return dataStreamName.substring(0, indexOfFirstDash);
    }

    private static String parseDataStreamDataset(String dataStreamName, int indexOfFirstDash, int indexOfSecondDash) {
        return dataStreamName.substring(indexOfFirstDash + 1, indexOfSecondDash);
    }

    private static String parseDataStreamNamespace(String dataStreamName, int indexOfSecondDash) {
        return dataStreamName.substring(indexOfSecondDash + 1);
    }

    private String determineDataStreamField(
        IngestDocument ingestDocument,
        List<DataStreamValueSource> valueSources,
        DataStreamValueSource dataStreamFieldReference,
        String fromCurrentTarget
    ) {
        for (DataStreamValueSource value : valueSources) {
            String result = value.resolve(ingestDocument);
            if (result != null) {
                return result;
            }
        }
        String fromDataStreamField = dataStreamFieldReference.resolve(ingestDocument);
        if (fromDataStreamField != null) {
            return fromDataStreamField;
        }
        return fromCurrentTarget;
    }

    @Override
    public String getType() {
        return TYPE;
    }

    List<DataStreamValueSource> getDataStreamDataset() {
        return dataset;
    }

    List<DataStreamValueSource> getDataStreamNamespace() {
        return namespace;
    }

    String getDestination() {
        return destination;
    }

    public static final class DataStreamValueSource {
        private final String value;
        private final String fieldReference;
        private final Function<String, String> sanitizer;

        public static DataStreamValueSource dataset(String dataset) {
            return new DataStreamValueSource(dataset, ds -> sanitizeDataStreamField(ds, DISALLOWED_IN_DATASET));
        }

        public static DataStreamValueSource namespace(String namespace) {
            return new DataStreamValueSource(namespace, nsp -> sanitizeDataStreamField(nsp, DISALLOWED_IN_NAMESPACE));
        }

        private DataStreamValueSource(String value, Function<String, String> sanitizer) {
            this.sanitizer = sanitizer;
            this.value = value;
            if (value.contains("{{") || value.contains("}}")) {
                if (value.startsWith("{{") == false || value.endsWith("}}") == false) {
                    throw new IllegalArgumentException("'" + value + "' is not a valid field reference");
                }
                String fieldReference = value.substring(2, value.length() - 2);
                // field references may have two or three curly braces
                if (fieldReference.startsWith("{") && fieldReference.endsWith("}")) {
                    fieldReference = fieldReference.substring(1, fieldReference.length() - 1);
                }
                // only a single field reference is allowed
                // so something like this is disallowed: {{foo}}-{{bar}}
                if (fieldReference.contains("{") || fieldReference.contains("}")) {
                    throw new IllegalArgumentException("'" + value + "' is not a valid field reference");
                }
                this.fieldReference = fieldReference;
            } else {
                this.fieldReference = null;
                if (Objects.equals(sanitizer.apply(value), value) == false) {
                    throw new IllegalArgumentException("'" + value + "' contains disallowed characters");
                }
            }
        }

        public String resolve(IngestDocument ingestDocument) {
            if (fieldReference != null) {
                try {
                    return sanitizer.apply(ingestDocument.getFieldValue(fieldReference, String.class, true));
                } catch (IllegalArgumentException e) {
                    return null;
                }
            } else {
                return value;
            }
        }

        @Override
        public String toString() {
            return value;
        }
    }

    public static final class Factory implements Processor.Factory {

        @Override
        public RerouteProcessor create(
            Map<String, Processor.Factory> processorFactories,
            String tag,
            String description,
            Map<String, Object> config
        ) throws Exception {
            List<DataStreamValueSource> dataset;
            try {
                dataset = ConfigurationUtils.readOptionalListOrString(TYPE, tag, config, "dataset")
                    .stream()
                    .map(ds -> DataStreamValueSource.dataset(ds))
                    .collect(Collectors.toList());
            } catch (IllegalArgumentException e) {
                throw newConfigurationException(TYPE, tag, "dataset", e.getMessage());
            }
            List<DataStreamValueSource> namespace;
            try {
                namespace = ConfigurationUtils.readOptionalListOrString(TYPE, tag, config, "namespace")
                    .stream()
                    .map(ds -> DataStreamValueSource.namespace(ds))
                    .collect(Collectors.toList());
            } catch (IllegalArgumentException e) {
                throw newConfigurationException(TYPE, tag, "namespace", e.getMessage());
            }

            String destination = ConfigurationUtils.readOptionalStringProperty(TYPE, tag, config, "destination");
            if (destination != null && (dataset.isEmpty() == false || namespace.isEmpty() == false)) {
                throw newConfigurationException(TYPE, tag, "destination", "can only be set if dataset and namespace are not set");
            }

            return new RerouteProcessor(tag, description, dataset, namespace, destination);
        }
    }
}
