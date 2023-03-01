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

import java.util.Locale;
import java.util.Map;
import java.util.Objects;

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
    private final String dataset;
    private final String namespace;
    private final String destination;

    RerouteProcessor(String tag, String description, String dataset, String namespace, String destination) {
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
        final String datasetFallback;
        final String namespaceFallback;
        int indexOfFirstDash = indexName.indexOf('-');
        if (indexOfFirstDash < 0) {
            throw createInvalidDataStreamNameException(indexName);
        }
        int indexOfSecondDash = indexName.indexOf('-', indexOfFirstDash + 1);
        if (indexOfSecondDash < 0) {
            throw createInvalidDataStreamNameException(indexName);
        }
        type = parseDataStreamType(indexName, indexOfFirstDash);
        datasetFallback = parseDataStreamDataset(indexName, indexOfFirstDash, indexOfSecondDash);
        namespaceFallback = parseDataStreamNamespace(indexName, indexOfSecondDash);

        String dataset = getDataset(ingestDocument, datasetFallback);
        String namespace = getNamespace(ingestDocument, namespaceFallback);
        ingestDocument.setFieldValue(DATA_STREAM_TYPE, type);
        ingestDocument.setFieldValue(DATA_STREAM_DATASET, dataset);
        ingestDocument.setFieldValue(DATA_STREAM_NAMESPACE, namespace);
        ingestDocument.reroute(type + "-" + dataset + "-" + namespace);
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

    private String getDataset(IngestDocument ingestDocument, String datasetFallback) {
        String dataset = this.dataset;
        if (dataset == null) {
            dataset = sanitizeDataStreamField(ingestDocument.getFieldValue(DATA_STREAM_DATASET, String.class, true), DISALLOWED_IN_DATASET);
        }
        if (dataset == null) {
            dataset = datasetFallback;
        }
        return dataset;
    }

    private String getNamespace(IngestDocument ingestDocument, String namespaceFallback) {
        String namespace = this.namespace;
        if (namespace == null) {
            namespace = sanitizeDataStreamField(
                ingestDocument.getFieldValue(DATA_STREAM_NAMESPACE, String.class, true),
                DISALLOWED_IN_NAMESPACE
            );
        }
        if (namespace == null) {
            namespace = namespaceFallback;
        }
        return namespace;
    }

    @Override
    public String getType() {
        return TYPE;
    }

    public String getDataStreamDataset() {
        return dataset;
    }

    public String getDataStreamNamespace() {
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
            String dataset = ConfigurationUtils.readOptionalStringProperty(TYPE, tag, config, "dataset");
            if (Objects.equals(sanitizeDataStreamField(dataset, DISALLOWED_IN_DATASET), dataset) == false) {
                throw newConfigurationException(TYPE, tag, "dataset", "contains illegal characters");
            }
            String namespace = ConfigurationUtils.readOptionalStringProperty(TYPE, tag, config, "namespace");
            if (Objects.equals(sanitizeDataStreamField(namespace, DISALLOWED_IN_NAMESPACE), namespace) == false) {
                throw newConfigurationException(TYPE, tag, "namespace", "contains illegal characters");
            }
            String destination = ConfigurationUtils.readOptionalStringProperty(TYPE, tag, config, "destination");
            if (destination != null && (dataset != null || namespace != null)) {
                throw newConfigurationException(TYPE, tag, "destination", "can only be set if dataset and namespace are not set");
            }

            return new RerouteProcessor(tag, description, dataset, namespace, destination);
        }
    }
}
