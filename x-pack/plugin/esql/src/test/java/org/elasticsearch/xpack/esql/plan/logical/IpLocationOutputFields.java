/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.plan.logical;

import org.elasticsearch.ingest.geoip.Database;
import org.elasticsearch.iplocation.api.DatabaseProperty;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.docs.OutputFields;
import org.elasticsearch.xpack.esql.expression.function.DocsV3Support;

import java.io.IOException;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Set;
import java.util.SortedMap;
import java.util.TreeMap;

/**
 * Defines the `output` fields for {@code IP_LOCATION}. {@link DocsV3Support} finds this class via
 * reflection by naming convention, then calls {@link #renderOutput} directly.
 */
public class IpLocationOutputFields {

    /**
     * Glob patterns for known ip location database file names, in the order they should be rendered.
     * MaxMind databases are matched by filename suffix (reliable); ipinfo databases are matched heuristically
     * by substring, since ipinfo file names are not standardized.
     */
    public static final LinkedHashMap<String, Database> DATABASE_GLOBS = new LinkedHashMap<>();
    static {
        DATABASE_GLOBS.put("*-City.mmdb", Database.City);
        DATABASE_GLOBS.put("*-Country.mmdb", Database.Country);
        DATABASE_GLOBS.put("*-ASN.mmdb", Database.Asn);
        DATABASE_GLOBS.put("*-Anonymous-IP.mmdb", Database.AnonymousIp);
        DATABASE_GLOBS.put("*-Connection-Type.mmdb", Database.ConnectionType);
        DATABASE_GLOBS.put("*-Domain.mmdb", Database.Domain);
        DATABASE_GLOBS.put("*-Enterprise.mmdb", Database.Enterprise);
        DATABASE_GLOBS.put("*-ISP.mmdb", Database.Isp);
        DATABASE_GLOBS.put("ipinfo*plus*.mmdb", Database.IpinfoPlus);
        DATABASE_GLOBS.put("ipinfo*asn*.mmdb", Database.AsnV2);
        DATABASE_GLOBS.put("ipinfo*country*.mmdb", Database.CountryV2);
        DATABASE_GLOBS.put("ipinfo*location*.mmdb", Database.CityV2);
        DATABASE_GLOBS.put("ipinfo*privacy*.mmdb", Database.PrivacyDetection);
    }

    /**
     * Entry point called by {@link DocsV3Support.CommandsDocsSupport} via reflection. Delegates to
     * {@link OutputFields#renderDatabaseFileOutputBlock} with normal parameters.
     */
    public static void renderOutput(XContentBuilder builder) throws IOException {
        OutputFields.renderDatabaseFileOutputBlock(builder, DATABASE_GLOBS, IpLocationOutputFields::renderVariantFields);
    }

    /**
     * Renders one IP_LOCATION output variant's fields (name, type, and "default": false
     * for fields available but not selected by default) for the given database kind.
     */
    public static void renderVariantFields(XContentBuilder builder, Database database) throws IOException {
        Set<DatabaseProperty> defaultProperties = database.defaultProperties();
        SortedMap<String, DatabaseProperty> sortedProperties = new TreeMap<>();
        for (DatabaseProperty property : database.properties()) {
            sortedProperties.put(property.fieldName(), property);
        }
        for (Map.Entry<String, DatabaseProperty> entry : sortedProperties.entrySet()) {
            DatabaseProperty property = entry.getValue();
            DataType dataType = DataType.fromJavaType(property.fieldType());
            if (dataType == null && DatabaseProperty.LOCATION.fieldName().equals(entry.getKey())) {
                dataType = DataType.GEO_POINT;
            }
            builder.startObject(entry.getKey());
            builder.field("type", dataType.esNameIfPossible());
            if (defaultProperties.contains(property) == false) {
                builder.field("default", false);
            }
            builder.endObject();
        }
    }
}
