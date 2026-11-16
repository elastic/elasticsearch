/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.parquet.write.table;

import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentFactory;
import org.elasticsearch.xcontent.XContentType;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * The entry of a table in the file-based catalog: the name of the table, where it is, and which metadata file is current. One entry is
 * one small JSON file, see {@link RepositoryCatalog}. The entry is a pointer and not the commit of the table, so it can lag behind the
 * newest {@code v<N>.metadata.json}, and a reader that wants the newest table walks forward from it.
 *
 * @param namespace                the namespace of the table, one string for each level
 * @param name                     the name of the table, which is the name of the index
 * @param tableUuid                the uuid that the Iceberg metadata gives the table
 * @param location                 the URI of the root of the table
 * @param metadataLocation         the URI of the table metadata file that was current when the entry was written
 * @param previousMetadataLocation the one before it, or null for the first
 * @param metadataVersion          the number {@code N} of {@code v<N>.metadata.json} in {@code metadataLocation}
 * @param createdAtMillis          when the first entry of this table was written
 * @param updatedAtMillis          when this entry was written
 * @param index                    the name of the index that writes the table
 * @param indexUuid                the uuid of that index
 * @param clusterUuid              the uuid of the cluster, to tell apart two clusters that write the same name to one repository
 */
public record CatalogEntry(
    List<String> namespace,
    String name,
    String tableUuid,
    String location,
    String metadataLocation,
    @Nullable String previousMetadataLocation,
    int metadataVersion,
    long createdAtMillis,
    long updatedAtMillis,
    String index,
    String indexUuid,
    String clusterUuid
) {

    public static final String FORMAT = "elasticsearch-file-catalog";
    public static final int FORMAT_VERSION = 1;

    /** The same table with another current metadata file. */
    public CatalogEntry withMetadata(String newMetadataLocation, int newMetadataVersion, @Nullable String previous, long updatedAt) {
        return new CatalogEntry(
            namespace,
            name,
            tableUuid,
            location,
            newMetadataLocation,
            previous,
            newMetadataVersion,
            createdAtMillis,
            updatedAt,
            index,
            indexUuid,
            clusterUuid
        );
    }

    /** The same entry with the creation time of an earlier entry of the same table. */
    CatalogEntry withCreatedAt(long createdAt) {
        return new CatalogEntry(
            namespace,
            name,
            tableUuid,
            location,
            metadataLocation,
            previousMetadataLocation,
            metadataVersion,
            createdAt,
            updatedAtMillis,
            index,
            indexUuid,
            clusterUuid
        );
    }

    public BytesReference toBytes() throws IOException {
        try (XContentBuilder builder = XContentFactory.jsonBuilder().prettyPrint()) {
            builder.startObject();
            builder.field("format", FORMAT);
            builder.field("format_version", FORMAT_VERSION);
            builder.field("namespace", namespace);
            builder.field("name", name);
            builder.field("table_uuid", tableUuid);
            builder.field("location", location);
            builder.field("metadata_location", metadataLocation);
            if (previousMetadataLocation != null) {
                builder.field("previous_metadata_location", previousMetadataLocation);
            }
            builder.field("metadata_version", metadataVersion);
            builder.field("created_at_ms", createdAtMillis);
            builder.field("updated_at_ms", updatedAtMillis);
            builder.startObject("elasticsearch");
            builder.field("index", index);
            builder.field("index_uuid", indexUuid);
            builder.field("cluster_uuid", clusterUuid);
            builder.endObject();
            builder.endObject();
            return BytesReference.bytes(builder);
        }
    }

    /** Reads an entry. Fields that this version does not know are ignored, so that the format can grow. */
    public static CatalogEntry parse(BytesReference bytes) {
        final Map<String, Object> map = XContentHelper.convertToMap(bytes, false, XContentType.JSON).v2();
        if (FORMAT.equals(map.get("format")) == false) {
            throw new IllegalArgumentException("not a catalog entry of this format: [" + map.get("format") + "]");
        }
        final Object version = map.get("format_version");
        if (version instanceof Number number && number.intValue() > FORMAT_VERSION) {
            throw new IllegalArgumentException(
                "the catalog entry has the format version [" + version + "], and this code reads [" + FORMAT_VERSION + "]"
            );
        }
        final List<String> namespace = new ArrayList<>();
        if (map.get("namespace") instanceof List<?> levels) {
            for (Object level : levels) {
                namespace.add(String.valueOf(level));
            }
        }
        final Map<?, ?> source = map.get("elasticsearch") instanceof Map<?, ?> m ? m : Map.of();
        return new CatalogEntry(
            namespace,
            string(map, "name"),
            string(map, "table_uuid"),
            string(map, "location"),
            string(map, "metadata_location"),
            map.get("previous_metadata_location") == null ? null : String.valueOf(map.get("previous_metadata_location")),
            ((Number) map.get("metadata_version")).intValue(),
            ((Number) map.get("created_at_ms")).longValue(),
            ((Number) map.get("updated_at_ms")).longValue(),
            String.valueOf(source.get("index")),
            String.valueOf(source.get("index_uuid")),
            String.valueOf(source.get("cluster_uuid"))
        );
    }

    private static String string(Map<String, Object> map, String key) {
        final Object value = map.get(key);
        if (value == null) {
            throw new IllegalArgumentException("the catalog entry has no [" + key + "]");
        }
        return value.toString();
    }
}
