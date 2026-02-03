/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.expression.promql.function;

import org.elasticsearch.common.Strings;
import org.elasticsearch.core.PathUtils;
import org.elasticsearch.logging.LogManager;
import org.elasticsearch.logging.Logger;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.json.JsonXContent;
import org.elasticsearch.xpack.esql.expression.function.DocsV3Support;

import java.io.IOException;
import java.nio.file.Path;
import java.util.Locale;
import java.util.Set;

/**
 * PromQL-specific doc generation infrastructure.
 */
public class PromqlDocsSupport extends DocsV3Support {

    private static final Logger logger = LogManager.getLogger(PromqlDocsSupport.class);

    private static final String DOCS_COMMENT = "PromQL function definition for Kibana. "
        + "See https://prometheus.io/docs/prometheus/latest/querying/functions/";

    private final PromqlFunctionRegistry.FunctionDefinition promqlDef;

    public PromqlDocsSupport(PromqlFunctionRegistry.FunctionDefinition def, Callbacks callbacks) {
        super("functions", def.name(), PromqlDocsSupport.class, Set::of, callbacks);
        this.promqlDef = def;
    }

    @Override
    protected void renderDocs() throws Exception {
        renderKibanaFunctionDefinition();
    }

    private void renderKibanaFunctionDefinition() throws Exception {
        try (XContentBuilder builder = JsonXContent.contentBuilder().prettyPrint().lfAtEnd()) {
            builder.startObject();
            builder.field("comment", DOCS_COMMENT);
            builder.field("type", mapType(promqlDef.functionType()));
            builder.field("name", promqlDef.name());
            builder.field("description", promqlDef.description());

            builder.startArray("signatures");
            builder.startObject();
            builder.startArray("params");
            for (PromqlFunctionRegistry.ParamInfo param : promqlDef.params()) {
                builder.startObject();
                builder.field("name", param.name());
                builder.field("type", param.type().name().toLowerCase(Locale.ROOT));
                builder.field("optional", param.optional());
                builder.field("description", param.description());
                builder.endObject();
            }
            builder.endArray();
            builder.field("variadic", false);
            builder.field("returnType", mapReturnType(promqlDef.functionType()));
            builder.endObject();
            builder.endArray();

            builder.array("examples", promqlDef.examples().toArray(String[]::new));
            builder.field("preview", true);
            builder.field("snapshot_only", false);
            builder.endObject();

            String rendered = Strings.toString(builder);
            logger.info("Writing PromQL kibana function definition for [{}]", promqlDef.name());
            writeToPromqlKibanaDir("definitions", "json", rendered);
        }
    }

    private void writeToPromqlKibanaDir(String subdir, String extension, String str) throws IOException {
        Path dir = PathUtils.get(System.getProperty("java.io.tmpdir")).resolve("promql").resolve("kibana").resolve(subdir);
        callbacks.write(dir, promqlDef.name(), extension, str, false);
    }

    private String mapType(FunctionType type) {
        return switch (type) {
            case WITHIN_SERIES_AGGREGATION -> "promql_within_series";
            case ACROSS_SERIES_AGGREGATION -> "promql_across_series";
            case VALUE_TRANSFORMATION -> "promql_value_transformation";
            case VECTOR_CONVERSION -> "promql_vector_conversion";
            case SCALAR -> "promql_scalar";
            case METADATA_MANIPULATION -> "promql_metadata";
            case TIME_EXTRACTION -> "promql_time";
            case HISTOGRAM -> "promql_histogram";
            case SCALAR_CONVERSION -> "promql_scalar_conversion";
        };
    }

    private String mapReturnType(FunctionType type) {
        return type.outputType().toString();
    }

    public static void generateAll(Callbacks callbacks) throws Exception {
        for (var def : PromqlFunctionRegistry.INSTANCE.allFunctions()) {
            new PromqlDocsSupport(def, callbacks).renderDocs();
        }
    }
}
