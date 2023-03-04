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

import java.util.Iterator;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.function.Function;

import static org.elasticsearch.ingest.ConfigurationUtils.newConfigurationException;

public final class RerouteProcessor extends AbstractProcessor {
    public static final String TYPE = "reroute";

    private static final String DATA_STREAM_PREFIX = "data_stream.";
    private static final String DATA_STREAM_TYPE = DATA_STREAM_PREFIX + "type";
    private static final String DATA_STREAM_DATASET = DATA_STREAM_PREFIX + "dataset";
    private static final String DATA_STREAM_NAMESPACE = DATA_STREAM_PREFIX + "namespace";
    private static final char[] DISALLOWED_IN_DATASET = new char[] { '\\', '/', '*', '?', '\"', '<', '>', '|', ' ', ',', '#', ':', '-' };
    private static final char[] DISALLOWED_IN_NAMESPACE = new char[] { '\\', '/', '*', '?', '\"', '<', '>', '|', ' ', ',', '#', ':' };
    private static final int MAX_LENGTH = 100;
    private static final char REPLACEMENT_CHAR = '_';
    private final List<String> dataset;
    private final List<String> namespace;
    private final String destination;

    RerouteProcessor(List<String> dataset, List<String> namespace) {
        this(null, null, dataset, namespace, null);
    }

    RerouteProcessor(String destination) {
        this(null, null, null, null, destination);
    }

    RerouteProcessor(String tag, String description, List<String> dataset, List<String> namespace, String destination) {
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

    private static String sanitizeDataset(String dataset) {
        return sanitizeDataStreamField(dataset, DISALLOWED_IN_DATASET);
    }

    private static String sanitizeNamespace(String namespace) {
        return sanitizeDataStreamField(namespace, DISALLOWED_IN_NAMESPACE);
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

        String dataset = determineDataset(ingestDocument, currentDataset);
        String namespace = determineNamespace(ingestDocument, currentNamespace);
        if (dataset == null || namespace == null) {
            return ingestDocument;
        }
        String newTarget = type + "-" + dataset + "-" + namespace;
        ingestDocument.reroute(newTarget);
        ingestDocument.setFieldValue(DATA_STREAM_TYPE, type);
        ingestDocument.setFieldValue(DATA_STREAM_DATASET, dataset);
        ingestDocument.setFieldValue(DATA_STREAM_NAMESPACE, namespace);
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

    private String determineDataset(IngestDocument ingestDocument, String currentDataset) {
        return determineDataStreamField(ingestDocument, dataset, currentDataset, RerouteProcessor::sanitizeDataset, DATA_STREAM_DATASET);
    }

    private String determineNamespace(IngestDocument ingestDocument, String currentNamespace) {
        return determineDataStreamField(
            ingestDocument,
            namespace,
            currentNamespace,
            RerouteProcessor::sanitizeNamespace,
            DATA_STREAM_NAMESPACE
        );
    }

    private String determineDataStreamField(
        IngestDocument ingestDocument,
        List<String> valueSources,
        String fromCurrentTarget,
        Function<String, String> sanitization,
        String dataStreamFieldName
    ) {
        String result = null;
        for (Iterator<String> iterator = valueSources.iterator(); iterator.hasNext(); ) {
            String value = iterator.next();
            if (value.startsWith("{") && value.endsWith("}")) {
                String fieldReference = value.substring(1, value.length() - 1);
                result = sanitization.apply(ingestDocument.getFieldValue(fieldReference, String.class, true));
                if (fieldReference.equals(dataStreamFieldName) && fromCurrentTarget.equals(result)) {
                    result = null;
                }
            } else {
                result = value;
            }
            if (result != null) {
                break;
            }
        }
        if (result == null) {
            result = sanitization.apply(ingestDocument.getFieldValue(dataStreamFieldName, String.class, true));
        }
        if (result == null) {
            result = fromCurrentTarget;
        }
        return result;
    }

    @Override
    public String getType() {
        return TYPE;
    }

    public List<String> getDataStreamDataset() {
        return dataset;
    }

    public List<String> getDataStreamNamespace() {
        return namespace;
    }

    public String getDestination() {
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
            List<String> dataset = ConfigurationUtils.readOptionalListOrString(TYPE, tag, config, "dataset");
            dataset.stream()
                .filter(ds -> ds.startsWith("{") == false)
                .filter(ds -> Objects.equals(sanitizeDataset(ds), ds) == false)
                .findAny()
                .ifPresent(ds -> {
                    throw newConfigurationException(TYPE, tag, "dataset", "'" + ds + "' contains disallowed characters");
                });
            List<String> namespace = ConfigurationUtils.readOptionalListOrString(TYPE, tag, config, "namespace");
            namespace.stream()
                .filter(ns -> ns.startsWith("{") == false)
                .filter(ns -> Objects.equals(sanitizeNamespace(ns), ns) == false)
                .findAny()
                .ifPresent(ns -> {
                    throw newConfigurationException(TYPE, tag, "namespace", "'" + ns + "' contains disallowed characters");
                });

            String destination = ConfigurationUtils.readOptionalStringProperty(TYPE, tag, config, "destination");
            if (destination != null && (dataset.isEmpty() == false || namespace.isEmpty() == false)) {
                throw newConfigurationException(TYPE, tag, "destination", "can only be set if dataset and namespace are not set");
            }

            return new RerouteProcessor(tag, description, dataset, namespace, destination);
        }
    }
}
