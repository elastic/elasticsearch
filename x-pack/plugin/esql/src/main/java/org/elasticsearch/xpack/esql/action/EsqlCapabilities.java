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
import java.util.Locale;
import java.util.Set;

/**
 * A {@link Set} of "capabilities" supported by the {@link RestEsqlQueryAction}
 * and {@link RestEsqlAsyncQueryAction} APIs. These are exposed over the
 * {@link RestNodesCapabilitiesAction} and we use them to enable tests.
 */
public class EsqlCapabilities {
    public enum Cap {
        // Support for function {@code CBRT}. Done in #108574.
        FN_CBRT(false),

        // Support for {@code MV_APPEND} function. #107001
        FN_MV_APPEND(false),

        // Support for function {@code IP_PREFIX}.
        FN_IP_PREFIX(false),

        // Fix on function {@code SUBSTRING} that makes it not return null on empty strings.
        FN_SUBSTRING_EMPTY_NULL(false),

        // Support for aggregation function {@code TOP_LIST}.
        AGG_TOP_LIST(false),

        // Optimization for ST_CENTROID changed some results in cartesian data. #108713
        ST_CENTROID_AGG_OPTIMIZED(false),

        // Support for requesting the "_ignored" metadata field.
        METADATA_IGNORED_FIELD(false),

        // Support for the "LOOKUP" command.
        LOOKUP_COMMAND(true),

        // Support for the syntax {@code "tables": {"type": [<values>]}}.
        TABLES_TYPES(true),

        // Support for requesting the "REPEAT" command.
        REPEAT(false),

        // Cast string literals to datetime in addition and subtraction when the other side is a date or time interval.
        STRING_LITERAL_AUTO_CASTING_TO_DATETIME_ADD_SUB(false),

        // Support for named or positional parameters in EsqlQueryRequest.
        NAMED_POSITIONAL_PARAMETER(false);

        Cap(boolean snapshotOnly) {
            this.snapshotOnly = snapshotOnly;
        };

        private final boolean snapshotOnly;
    }

    public static final Set<String> CAPABILITIES = capabilities();

    private static Set<String> capabilities() {
        List<String> caps = new ArrayList<>();
        for (Cap cap : Cap.values()) {
            if (Build.current().isSnapshot() || cap.snapshotOnly == false) {
                caps.add(cap.name().toLowerCase(Locale.ROOT));
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
}
