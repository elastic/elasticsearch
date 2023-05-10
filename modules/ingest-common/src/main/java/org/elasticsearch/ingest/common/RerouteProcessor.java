/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.ingest.common;

import org.elasticsearch.core.Nullable;
import org.elasticsearch.ingest.AbstractProcessor;
import org.elasticsearch.ingest.ConfigurationUtils;
import org.elasticsearch.ingest.IngestDocument;
import org.elasticsearch.ingest.Processor;

import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.function.Function;
import java.util.regex.Pattern;

import static org.elasticsearch.core.Strings.format;
import static org.elasticsearch.ingest.ConfigurationUtils.newConfigurationException;
import static org.elasticsearch.ingest.common.RerouteProcessor.DataStreamValueSource.DATASET_VALUE_SOURCE;
import static org.elasticsearch.ingest.common.RerouteProcessor.DataStreamValueSource.NAMESPACE_VALUE_SOURCE;

public final class RerouteProcessor extends AbstractProcessor {

    public static final String TYPE = "reroute";

    private static final String NAMING_SCHEME_ERROR_MESSAGE =
        "invalid data stream name: [%s]; must follow naming scheme <type>-<dataset>-<namespace>";

    private static final String DATA_STREAM_PREFIX = "data_stream.";
    private static final String DATA_STREAM_TYPE = DATA_STREAM_PREFIX + "type";
    private static final String DATA_STREAM_DATASET = DATA_STREAM_PREFIX + "dataset";
    private static final String DATA_STREAM_NAMESPACE = DATA_STREAM_PREFIX + "namespace";
    private static final String EVENT_DATASET = "event.dataset";
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
        if (dataset.isEmpty()) {
            this.dataset = List.of(DATASET_VALUE_SOURCE);
        } else {
            this.dataset = dataset;
        }
        if (namespace.isEmpty()) {
            this.namespace = List.of(NAMESPACE_VALUE_SOURCE);
        } else {
            this.namespace = namespace;
        }
        this.destination = destination;
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

        // parse out the <type>-<dataset>-<namespace> components from _index
        int indexOfFirstDash = indexName.indexOf('-');
        if (indexOfFirstDash < 0) {
            throw new IllegalArgumentException(format(NAMING_SCHEME_ERROR_MESSAGE, indexName));
        }
        int indexOfSecondDash = indexName.indexOf('-', indexOfFirstDash + 1);
        if (indexOfSecondDash < 0) {
            throw new IllegalArgumentException(format(NAMING_SCHEME_ERROR_MESSAGE, indexName));
        }
        type = parseDataStreamType(indexName, indexOfFirstDash);
        currentDataset = parseDataStreamDataset(indexName, indexOfFirstDash, indexOfSecondDash);
        currentNamespace = parseDataStreamNamespace(indexName, indexOfSecondDash);

        String dataset = determineDataStreamField(ingestDocument, this.dataset, currentDataset);
        String namespace = determineDataStreamField(ingestDocument, this.namespace, currentNamespace);
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
        String fallbackFromCurrentTarget
    ) {
        // first try to get value from the configured dataset/namespace field references
        // if this contains a static value rather than a field reference, this is guaranteed to return
        for (DataStreamValueSource value : valueSources) {
            String result = value.resolve(ingestDocument);
            if (result != null) {
                return result;
            }
        }
        // use the dataset/namespace value we parsed out from the current target (_index) as a fallback
        return fallbackFromCurrentTarget;
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
                    .map(DataStreamValueSource::dataset)
                    .toList();
            } catch (IllegalArgumentException e) {
                throw newConfigurationException(TYPE, tag, "dataset", e.getMessage());
            }
            List<DataStreamValueSource> namespace;
            try {
                namespace = ConfigurationUtils.readOptionalListOrString(TYPE, tag, config, "namespace")
                    .stream()
                    .map(DataStreamValueSource::namespace)
                    .toList();
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

    /**
     * Contains either a {{field reference}} or a static value for a dataset or a namespace field
     */
    static final class DataStreamValueSource {

        private static final int MAX_LENGTH = 100;
        private static final String REPLACEMENT = "_";
        private static final Pattern DISALLOWED_IN_DATASET = Pattern.compile("[\\\\/*?\"<>| ,#:-]");
        private static final Pattern DISALLOWED_IN_NAMESPACE = Pattern.compile("[\\\\/*?\"<>| ,#:]");
        static final DataStreamValueSource DATASET_VALUE_SOURCE = dataset("{{" + DATA_STREAM_DATASET + "}}");
        static final DataStreamValueSource NAMESPACE_VALUE_SOURCE = namespace("{{" + DATA_STREAM_NAMESPACE + "}}");

        private final String value;
        private final String fieldReference;
        private final Function<String, String> sanitizer;

        public static DataStreamValueSource dataset(String dataset) {
            return new DataStreamValueSource(dataset, ds -> sanitizeDataStreamField(ds, DISALLOWED_IN_DATASET));
        }

        public static DataStreamValueSource namespace(String namespace) {
            return new DataStreamValueSource(namespace, nsp -> sanitizeDataStreamField(nsp, DISALLOWED_IN_NAMESPACE));
        }

        private static String sanitizeDataStreamField(String s, Pattern disallowedInDataset) {
            if (s == null) {
                return null;
            }
            s = s.toLowerCase(Locale.ROOT);
            s = s.substring(0, Math.min(s.length(), MAX_LENGTH));
            return disallowedInDataset.matcher(s).replaceAll(REPLACEMENT);
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

        /**
         * Resolves the field reference from the provided ingest document or returns the static value if this value source doesn't represent
         * a field reference.
         * @param ingestDocument
         * @return the resolved field reference or static value
         */
        @Nullable
        public String resolve(IngestDocument ingestDocument) {
            if (fieldReference != null) {
                return sanitizer.apply(ingestDocument.getFieldValue(fieldReference, String.class, true));
            } else {
                return value;
            }
        }
    }
}
