/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.ingest.iplocation;

import org.elasticsearch.cluster.metadata.ProjectId;
import org.elasticsearch.common.logging.DeprecationCategory;
import org.elasticsearch.common.logging.DeprecationLogger;
import org.elasticsearch.core.Strings;
import org.elasticsearch.ingest.AbstractProcessor;
import org.elasticsearch.ingest.IngestDocument;
import org.elasticsearch.ingest.Processor;
import org.elasticsearch.iplocation.api.IpDataLookup;
import org.elasticsearch.iplocation.api.IpLocationService;
import org.elasticsearch.iplocation.api.UnsupportedDatabaseTypeException;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.SequencedMap;
import java.util.Set;

import static org.elasticsearch.ingest.ConfigurationUtils.newConfigurationException;
import static org.elasticsearch.ingest.ConfigurationUtils.readBooleanProperty;
import static org.elasticsearch.ingest.ConfigurationUtils.readOptionalList;
import static org.elasticsearch.ingest.ConfigurationUtils.readStringProperty;
import static org.elasticsearch.ingest.IngestPipelineFieldAccessPattern.FLEXIBLE;

public final class GeoIpProcessor extends AbstractProcessor {

    private static final DeprecationLogger deprecationLogger = DeprecationLogger.getLogger(GeoIpProcessor.class);
    static final String UNSUPPORTED_DATABASE_DEPRECATION_MESSAGE = "the geoip processor will no longer support database type [{}] "
        + "in a future version of Elasticsearch";

    public static final String GEOIP_TYPE = "geoip";
    public static final String IP_LOCATION_TYPE = "ip_location";

    private final String type;
    private final String field;
    private final IpDataLookup ipDataLookup;
    private final String targetField;
    private final boolean ignoreMissing;
    private final boolean firstOnly;
    private final String databaseFile;

    /**
     * Construct a geo-IP processor.
     * @param type          the processor type (geoip or ip_location)
     * @param tag           the processor tag
     * @param description   the processor description
     * @param field         the source field to geo-IP map
     * @param ipDataLookup  a live lookup handle that encapsulates database loading, validity, and data extraction
     * @param targetField   the target field
     * @param ignoreMissing true if documents with a missing value for the field should be ignored
     * @param firstOnly     true if only first result should be returned in case of array
     * @param databaseFile  the name of the database file being queried; used only for tagging documents if the database is unavailable
     */
    GeoIpProcessor(
        String type,
        String tag,
        String description,
        String field,
        IpDataLookup ipDataLookup,
        String targetField,
        boolean ignoreMissing,
        boolean firstOnly,
        String databaseFile
    ) {
        super(tag, description);
        this.type = type;
        this.field = field;
        this.ipDataLookup = ipDataLookup;
        this.targetField = targetField;
        this.ignoreMissing = ignoreMissing;
        this.firstOnly = firstOnly;
        this.databaseFile = databaseFile;
    }

    boolean isIgnoreMissing() {
        return ignoreMissing;
    }

    @Override
    public IngestDocument execute(IngestDocument document) throws IOException {
        Object ip = document.getFieldValue(field, Object.class, ignoreMissing);

        if (ipDataLookup.isValid() == false) {
            document.appendFieldValue("tags", "_" + type + "_expired_database", false);
            return document;
        } else if (ip == null && ignoreMissing) {
            return document;
        } else if (ip == null) {
            throw new IllegalArgumentException("field [" + field + "] is null, cannot extract geoip information.");
        }

        if (ip instanceof String ipString) {
            Map<String, Object> data = ipDataLookup.lookup(ipString);
            if (data == null) {
                if (ignoreMissing == false) {
                    tag(document, type, databaseFile);
                }
                return document;
            }
            if (data.isEmpty() == false) {
                writeGeoIpData(document, targetField, data);
            }
        } else if (ip instanceof List<?> ipList) {
            boolean match = false;
            List<Map<String, Object>> dataList = new ArrayList<>(ipList.size());
            for (Object ipAddr : ipList) {
                if (ipAddr instanceof String == false) {
                    throw new IllegalArgumentException("array in field [" + field + "] should only contain strings");
                }
                Map<String, Object> data = ipDataLookup.lookup((String) ipAddr);
                if (data == null) {
                    if (ignoreMissing == false) {
                        tag(document, type, databaseFile);
                    }
                    return document;
                }
                if (data.isEmpty()) {
                    dataList.add(null);
                    continue;
                }
                if (firstOnly) {
                    writeGeoIpData(document, targetField, data);
                    return document;
                }
                match = true;
                dataList.add(data);
            }
            if (match) {
                writeGeoIpDataList(document, targetField, dataList);
            }
        } else {
            throw new IllegalArgumentException("field [" + field + "] should contain only string or array of strings");
        }

        return document;
    }

    @Override
    public String getType() {
        return type;
    }

    String getField() {
        return field;
    }

    String getTargetField() {
        return targetField;
    }

    String getDatabaseType() {
        return ipDataLookup.getInfo().getDatabaseType();
    }

    SequencedMap<String, Class<?>> getProperties() {
        return ipDataLookup.getInfo().getFields();
    }

    /**
     * Writes GeoIP data to the document. In flexible field access mode, writes individual dotted fields
     * (e.g., "my.field.city", "my.field.country") instead of a single nested object. The "location" field
     * is written as an array [lon, lat] for better compatibility with geo_point fields in flexible mode.
     *
     * @param document the ingest document
     * @param targetField the base target field path
     * @param data the GeoIP data to write
     */
    private void writeGeoIpData(IngestDocument document, String targetField, Map<String, Object> data) {
        if (document.getCurrentAccessPatternSafe() == FLEXIBLE) {
            // In flexible mode, write each property as a separate dotted field
            for (Map.Entry<String, Object> entry : data.entrySet()) {
                String key = entry.getKey();
                Object value = transformValueForFlexibleMode(key, entry.getValue());
                document.setFieldValue(targetField + "." + key, value);
            }
        } else {
            // In classic mode, write as a single nested object
            document.setFieldValue(targetField, data);
        }
    }

    /**
     * Writes a list of GeoIP data to the document. In flexible field access mode, writes each property
     * as a separate list (e.g., "my.field.city" contains a list of cities, one per IP).
     * In classic mode, writes as a single list of maps.
     *
     * @param document the ingest document
     * @param targetField the base target field path
     * @param dataList the list of GeoIP data to write
     */
    private void writeGeoIpDataList(IngestDocument document, String targetField, List<Map<String, Object>> dataList) {
        if (document.getCurrentAccessPatternSafe() == FLEXIBLE) {
            // In flexible mode, transpose the list of maps into separate lists per property
            // Collect all unique keys across all maps
            Set<String> allKeys = new java.util.HashSet<>();
            for (Map<String, Object> data : dataList) {
                if (data != null) {
                    allKeys.addAll(data.keySet());
                }
            }
            // For each key, build a list of values
            for (String key : allKeys) {
                List<Object> valuesList = new ArrayList<>(dataList.size());
                for (Map<String, Object> data : dataList) {
                    if (data == null) {
                        valuesList.add(null);
                    } else {
                        Object value = transformValueForFlexibleMode(key, data.get(key));
                        valuesList.add(value);
                    }
                }
                document.setFieldValue(targetField + "." + key, valuesList);
            }
        } else {
            // In classic mode, write as a single list of maps
            document.setFieldValue(targetField, dataList);
        }
    }

    /**
     * Transforms a GeoIP value for flexible field access mode.
     * Converts location maps to [lon, lat] arrays and validates that only location fields contain Maps.
     *
     * @param key the property key
     * @param value the property value
     * @return the transformed value suitable for flexible mode
     */
    @SuppressWarnings("unchecked")
    private static Object transformValueForFlexibleMode(String key, Object value) {
        // Convert location from map {lat, lon} to array [lon, lat] in flexible mode
        if ("location".equals(key) && value instanceof Map) {
            Map<String, Object> locationMap = (Map<String, Object>) value;
            Double lat = (Double) locationMap.get("lat");
            Double lon = (Double) locationMap.get("lon");
            return newMutableLocationList(lon, lat);
        } else {
            // Assert that we don't have any unexpected Map values (only location should be a Map)
            assert value instanceof Map == false : "unexpected Map value for key [" + key + "]";
        }
        return value;
    }

    /**
     * Creates a mutable list containing [lon, lat] coordinates.
     * Using ArrayList instead of List.of() to allow users to modify the location field later
     * with other processors (e.g., script processor).
     * @param lon the longitude
     * @param lat the latitude
     * @return a mutable ArrayList containing [lon, lat]
     */
    private static List<Double> newMutableLocationList(double lon, double lat) {
        List<Double> location = new ArrayList<>(2);
        location.add(lon);
        location.add(lat);
        return location;
    }

    public static final class Factory implements Processor.Factory {

        private final String type;
        private final IpLocationService ipLocationService;

        public Factory(String type, IpLocationService ipLocationService) {
            this.type = type;
            this.ipLocationService = ipLocationService;
        }

        @Override
        public Processor create(
            Map<String, Processor.Factory> registry,
            String processorTag,
            String description,
            Map<String, Object> config,
            ProjectId projectId
        ) throws IOException {
            String ipField = readStringProperty(type, processorTag, config, "field");
            String targetField = readStringProperty(type, processorTag, config, "target_field", type);
            String databaseFile = readStringProperty(type, processorTag, config, "database_file", "GeoLite2-City.mmdb");
            List<String> propertyNames = readOptionalList(type, processorTag, config, "properties");
            boolean ignoreMissing = readBooleanProperty(type, processorTag, config, "ignore_missing", false);
            boolean firstOnly = readBooleanProperty(type, processorTag, config, "first_only", true);

            // Consume the property to prevent "unknown property" validation error.
            // The actual logic is handled by IngestIpLocationPlugin.hasAtLeastOneGeoipProcessor().
            readBooleanProperty(type, processorTag, config, "download_database_on_pipeline_creation", true);

            final IpDataLookup lookup;
            try {
                lookup = ipLocationService.createIpDataLookup(projectId.id(), databaseFile, propertyNames);
            } catch (UnsupportedDatabaseTypeException e) {
                throw newConfigurationException(type, processorTag, "database_file", e.getMessage());
            } catch (IllegalArgumentException e) {
                throw newConfigurationException(type, processorTag, "properties", e.getMessage());
            }
            if (lookup == null) {
                // It's possible that the database could be downloaded via the GeoipDownloader process and could become available
                // at a later moment, so a processor impl is returned that tags documents instead. If a database cannot be sourced
                // then the processor will continue to tag documents with a warning until it is remediated by providing a database
                // or changing the pipeline.
                return new DatabaseUnavailableProcessor(type, processorTag, description, databaseFile);
            }

            String databaseType = lookup.getInfo().getDatabaseType();
            if (databaseType == null) {
                throw newConfigurationException(
                    type,
                    processorTag,
                    "database_file",
                    Strings.format("Unsupported database type [null] for file [%s]", databaseFile)
                );
            }
            // the "geoip" processor type does additional validation of the database_type
            if (GEOIP_TYPE.equals(type)) {
                // type sniffing is done with the lowercased type
                String lower = databaseType.toLowerCase(Locale.ROOT);
                // start with a strict positive rejection check -- as we support addition database providers,
                // we should expand these checks when possible
                if (lower.startsWith("ipinfo")) {
                    throw newConfigurationException(
                        type,
                        processorTag,
                        "database_file",
                        Strings.format("Unsupported database type [%s] for file [%s]", databaseType, databaseFile)
                    );
                }
                // end with a lax negative rejection check -- if we aren't *certain* it's a maxmind database, then we'll warn --
                // it's possible for example that somebody cooked up a custom database of their own that happened to work with
                // our preexisting code, they should migrate to the new processor, but we're not going to break them right now
                if (lower.startsWith("geoip2") == false && lower.startsWith("geolite2") == false) {
                    deprecationLogger.warn(
                        DeprecationCategory.OTHER,
                        "unsupported_database_type",
                        UNSUPPORTED_DATABASE_DEPRECATION_MESSAGE,
                        databaseType
                    );
                }
            }

            return new GeoIpProcessor(
                type,
                processorTag,
                description,
                ipField,
                lookup,
                targetField,
                ignoreMissing,
                firstOnly,
                databaseFile
            );
        }

        /**
         * Get the value of the "download_database_on_pipeline_creation" property from a processor's config map.
         * <p>
         * As with the actual property definition, the default value of the property is 'true'. Unlike the actual
         * property definition, this method doesn't consume (that is, <code>config.remove</code>) the property from
         * the config map.
         */
        public static boolean downloadDatabaseOnPipelineCreation(Map<String, Object> config) {
            return (boolean) config.getOrDefault("download_database_on_pipeline_creation", true);
        }
    }

    private static void tag(IngestDocument ingestDocument, String type, String databaseName) {
        ingestDocument.appendFieldValue("tags", "_" + type + "_database_unavailable_" + databaseName, true);
    }
}
