/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.mapper;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.join.BitSetProducer;
import org.elasticsearch.core.Booleans;
import org.elasticsearch.index.IndexVersion;
import org.elasticsearch.index.IndexVersions;
import org.elasticsearch.index.query.SearchExecutionContext;

import java.util.Map;
import java.util.function.Function;

/**
 * An abstract {@link MetadataFieldMapper} used as a placeholder for implementation
 * in the inference module. It is required by {@link SourceFieldMapper} to identify
 * the field name for removal from _source.
 */
public abstract class InferenceMetadataFieldsMapper extends MetadataFieldMapper {
    public static class SystemProperty {
        private static final Logger logger = LogManager.getLogger(SystemProperty.class);

        private final boolean enabled;

        private SystemProperty(String name) {
            String propertyName = "es." + name;
            this.enabled = parseSystemProperty(propertyName, false);
            if (this.enabled) {
                logger.info("Feature " + name + " (via system property '" + propertyName + "') is enabled");
            } else {
                logger.debug("Feature " + name + " (via system property '" + propertyName + "') is disabled");
            }
        }

        private boolean parseSystemProperty(String propertyName, boolean defaultValue) {
            final String propertyValue = System.getProperty(propertyName);
            logger.trace("System property [{}] is set to [{}]", propertyName, propertyValue);
            try {
                return Booleans.parseBoolean(propertyValue, defaultValue);
            } catch (IllegalArgumentException e) {
                throw new IllegalArgumentException("Invalid value [" + propertyValue + "] for system property [" + propertyName + "]", e);
            }
        }

        public boolean isEnabled() {
            return enabled;
        }
    }

    public static final SystemProperty INFERENCE_METADATA_FIELDS_SYSTEM_PROPERTY = new SystemProperty("inference_metadata_fields");

    public static final String NAME = "_inference_fields";
    public static final String CONTENT_TYPE = "_inference_fields";

    protected InferenceMetadataFieldsMapper(MappedFieldType inferenceFieldType) {
        super(inferenceFieldType);
    }

    @Override
    protected String contentType() {
        return CONTENT_TYPE;
    }

    @Override
    public InferenceMetadataFieldType fieldType() {
        return (InferenceMetadataFieldType) super.fieldType();
    }

    public abstract static class InferenceMetadataFieldType extends MappedFieldType {
        public InferenceMetadataFieldType() {
            super(NAME, false, false, false, TextSearchInfo.NONE, Map.of());
        }

        /**
         * Returns a {@link ValueFetcher} without requiring the construction of a full {@link SearchExecutionContext}.
         */
        public abstract ValueFetcher valueFetcher(
            MappingLookup mappingLookup,
            Function<Query, BitSetProducer> bitSetCache,
            IndexSearcher searcher
        );
    }

    public static boolean isEnabled(IndexVersion indexVersion) {
        return indexVersion.onOrAfter(IndexVersions.INFERENCE_METADATA_FIELDS) && INFERENCE_METADATA_FIELDS_SYSTEM_PROPERTY.isEnabled();
    }
}
