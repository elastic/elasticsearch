/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.action;

import org.elasticsearch.Build;
import org.elasticsearch.features.NodeFeature;
import org.elasticsearch.rest.action.admin.cluster.RestNodesCapabilitiesAction;
import org.elasticsearch.xpack.esql.plugin.EsqlFeatures;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;

/**
 * A {@link Set} of "capabilities" supported by the {@link RestEsqlQueryAction}
 * and {@link RestEsqlAsyncQueryAction} APIs. These are exposed over the
 * {@link RestNodesCapabilitiesAction} and we use them to enable tests.
 */
public enum EsqlCapabilities {
    /**
     * Support for function {@code CBRT}. Done in #108574.
     */
    FN_CBRT("fn_cbrt"),
    /**
     * Support for {@code MV_APPEND} function. #107001
     */
    FN_MV_APPEND("fn_mv_append"),
    /**
     * Support for function {@code IP_PREFIX}.
     */
    FN_IP_PREFIX("fn_ip_prefix"),
    /**
     * Fix on function {@code SUBSTRING} that makes it not return null on empty strings.
     */
    FN_SUBSTRING_EMPTY_NULL("fn_substring_empty_null"),
    /**
     * Support for aggregation function {@code TOP_LIST}.
     */
    AGG_TOP_LIST("agg_top_list"),
    /**
     * Optimization for ST_CENTROID changed some results in cartesian data. #108713
     */
    ST_CENTROID_AGG_OPTIMIZED("st_centroid_agg_optimized"),
    /**
     * Support for requesting the "_ignored" metadata field.
     */
    METADATA_IGNORED_FIELD("metadata_field_ignored"),
    /**
     * Support for the "LOOKUP" command.
     */
    LOOKUP_COMMAND("lookup_command", true),
    /**
     * Support for the syntax {@code "tables": {"type": [<values>]}}.
     */
    TABLES_TYPES("tables_types", true),
    /**
     * Support for requesting the "REPEAT" command.
     */
    REPEAT("repeat");

    /**
     * Cast string literals to datetime in addition and subtraction when the other side is a date or time interval.
     */
    public static final String STRING_LITERAL_AUTO_CASTING_TO_DATETIME_ADD_SUB = "string_literal_auto_casting_to_datetime_add_sub";

    /**
     * Support for named or positional parameters in EsqlQueryRequest.
     */
    private static final String NAMED_POSITIONAL_PARAMETER = "named_positional_parameter";

    public static final Set<String> CAPABILITIES = capabilities();

    private static Set<String> capabilities() {
        List<String> caps = new ArrayList<>();
        for (EsqlCapabilities cap: EsqlCapabilities.values()) {
            if (Build.current().isSnapshot() || cap.snapshotOnly == false) {
                caps.add(cap.name);
            }
        }

        /*
         * Add all of our cluster features without the leading "esql."
         */
        for (NodeFeature feature : new EsqlFeatures().getFeatures()) {
            caps.add(cap(feature));
        }
        for (NodeFeature feature : new EsqlFeatures().getHistoricalFeatures().keySet()) {
            caps.add(cap(feature));
        }
        caps.add(STRING_LITERAL_AUTO_CASTING_TO_DATETIME_ADD_SUB);
        return Set.copyOf(caps);
    }

    /**
     * Convert a {@link NodeFeature} from {@link EsqlFeatures} into a
     * capability.
     */
    public static String cap(NodeFeature feature) {
        assert feature.id().startsWith("esql.");
        return feature.id().substring("esql.".length());
    }

    private final String name;
    private final boolean snapshotOnly;

    EsqlCapabilities(String name, boolean snapshotOnly) {
        this.name = name;
        this.snapshotOnly = snapshotOnly;
    }

    EsqlCapabilities(String name){
        this(name, false);
    }
}
