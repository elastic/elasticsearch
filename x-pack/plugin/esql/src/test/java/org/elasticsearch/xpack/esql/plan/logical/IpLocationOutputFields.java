/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.plan.logical;

import org.elasticsearch.ingest.geoip.Database;
import org.elasticsearch.iplocation.api.DatabaseProperty;
import org.elasticsearch.iplocation.api.IpDatabaseFileGlobs;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xpack.esql.core.expression.Attribute;
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.expression.Nullability;
import org.elasticsearch.xpack.esql.core.expression.ReferenceAttribute;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.docs.OutputFields;
import org.elasticsearch.xpack.esql.expression.function.DocsV3Support;
import org.elasticsearch.xpack.esql.plan.logical.local.LocalRelation;

import java.io.IOException;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.SequencedMap;
import java.util.Set;
import java.util.SortedMap;
import java.util.TreeMap;

/**
 * Defines the `output` fields for {@code IP_LOCATION}. {@link DocsV3Support} finds this class via
 * reflection by naming convention, then calls {@link #renderOutput} directly.
 */
public class IpLocationOutputFields {

    /**
     * Entry point called by {@link DocsV3Support.CommandsDocsSupport} via reflection. Builds the glob-to-{@link
     * Database} map at call time from the canonical, shared {@link IpDatabaseFileGlobs#DATABASE_VARIANT_GLOBS} table,
     * then delegates to {@link OutputFields#renderVariedOutputBlock}, passing IP_LOCATION's specific {@code vary_by}
     * ({@code "database_file"}) and {@code selected_by} ({@code "properties"}) values.
     */
    public static void renderOutput(XContentBuilder builder) throws IOException {
        LinkedHashMap<String, Database> databaseGlobs = new LinkedHashMap<>();
        for (var entry : IpDatabaseFileGlobs.DATABASE_VARIANT_GLOBS.entrySet()) {
            databaseGlobs.put(entry.getKey(), Database.valueOf(entry.getValue()));
        }
        OutputFields.renderVariedOutputBlock(
            builder,
            "database_file",
            "properties",
            databaseGlobs,
            IpLocationOutputFields::renderVariantFields
        );
    }

    /**
     * Renders one IP_LOCATION output variant's fields (name, type, and "default": false for fields available but not
     * selected by default) for the given database kind. Field name/type pairs are derived from a real {@link
     * IpLocation} instance built via {@link IpLocation#createInitialInstance} (the same production entry point
     * {@code LogicalPlanBuilder} uses), exercising the actual production conversion (including its LOCATION -&gt;
     * GEO_POINT special case) instead of re-deriving it.
     */
    public static void renderVariantFields(XContentBuilder builder, Database database) throws IOException {
        Set<DatabaseProperty> defaultProperties = database.defaultProperties();

        // mirrors the production, package-private IpDataLookupInfoImpl.toFieldMap, without needing that class
        SequencedMap<String, Class<?>> filteredOutputFields = new LinkedHashMap<>();
        database.properties().stream().sorted().forEach(property -> filteredOutputFields.put(property.fieldName(), property.fieldType()));

        Source source = Source.EMPTY;
        LogicalPlan child = new LocalRelation(Source.EMPTY, List.of(), null);
        Expression input = new ReferenceAttribute(Source.EMPTY, null, "input", DataType.KEYWORD, Nullability.TRUE, null, false);
        Attribute outputFieldPrefix = new ReferenceAttribute(Source.EMPTY, null, "prefix", DataType.KEYWORD, Nullability.TRUE, null, false);
        IpLocation ipLocation = IpLocation.createInitialInstance(
            source,
            child,
            input,
            outputFieldPrefix,
            "<unused>",
            false,
            filteredOutputFields
        );

        List<String> names = ipLocation.outputFieldNames();
        List<Attribute> attrs = ipLocation.generatedAttributes();
        SortedMap<String, DataType> fieldTypesByName = new TreeMap<>();
        for (int i = 0; i < names.size(); i++) {
            fieldTypesByName.put(names.get(i), attrs.get(i).dataType());
        }
        for (Map.Entry<String, DataType> entry : fieldTypesByName.entrySet()) {
            String fieldName = entry.getKey();
            builder.startObject(fieldName);
            builder.field("type", entry.getValue().esNameIfPossible());
            boolean isDefault = defaultProperties.stream().anyMatch(p -> p.fieldName().equals(fieldName));
            if (isDefault == false) {
                builder.field("default", false);
            }
            builder.endObject();
        }
    }
}
