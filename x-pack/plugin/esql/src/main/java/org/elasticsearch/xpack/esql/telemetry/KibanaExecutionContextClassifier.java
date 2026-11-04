/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.telemetry;

import org.elasticsearch.core.Nullable;

import java.util.List;
import java.util.Locale;
import java.util.Map;

/**
 * Tells which Kibana feature sent an ES|QL query, and whether a person chose to run it, from the execution context Kibana puts
 * in the {@code X-Opaque-Id} header. The result is always one of the closed {@link Feature} values, so no part of the header
 * (request ids, saved object ids, rule ids) can reach a counter.
 * <p>
 * Kibana sends {@code <requestId>;kibana:<node>[;<child node>...]}, where each node is {@code type:name:id} with every part
 * URI-encoded, and cuts the context after 1024 characters ({@code execution_context_container.ts} and
 * {@code execution_context_service.ts} in Kibana). The feature comes from the root node, the first one after {@code ;kibana:}.
 * Background work that runs as a Task Manager task has the task type in the root node's name, so it is told apart by task
 * type; a rule's type is also in its {@code alert:execute <ruleTypeId>} child.
 * <p>
 * Some callers can't be told apart here: Agent Builder and AI Assistant queries carry the host app's context, PDF and PNG
 * reports render a dashboard in a browser and look like a person viewing it, and prebuilt content looks like user content.
 * <p>
 * This runs once per query on the coordinator, on a header any client can set, so it is linear in the header length, decodes
 * at most {@link #MAX_NODE_LENGTH} characters of a node, and never throws.
 */
public final class KibanaExecutionContextClassifier {

    /**
     * Whether a person chose to run the query, or Elastic's code runs it on its own.
     */
    public enum Interaction {
        PERSON,
        BACKGROUND,
        UNKNOWN;

        private final String label = name().toLowerCase(Locale.ROOT);

        @Override
        public String toString() {
            return label;
        }
    }

    /**
     * The Kibana feature that sent the query. Discover, dashboard panels, stack and Security rules, Alerting v2 and CSV reports
     * were seen in a live check; the other values are matched on what Kibana's source sets.
     */
    public enum Feature {
        DISCOVER(Interaction.PERSON),
        DASHBOARDS(Interaction.PERSON),
        LENS(Interaction.PERSON),
        MAPS(Interaction.PERSON),
        CANVAS(Interaction.PERSON),
        DEV_TOOLS(Interaction.PERSON),
        STACK_RULE(Interaction.BACKGROUND),
        SECURITY_RULE(Interaction.BACKGROUND),
        OBSERVABILITY_RULE(Interaction.BACKGROUND),
        ALERTING_V2(Interaction.BACKGROUND),
        ENTITY_STORE(Interaction.BACKGROUND),
        REPORTING(Interaction.BACKGROUND),
        OTHER_TASK_MANAGER(Interaction.BACKGROUND),
        OTHER_KIBANA(Interaction.UNKNOWN);

        private final Interaction interaction;
        private final String label;

        Feature(Interaction interaction) {
            this.interaction = interaction;
            this.label = name().toLowerCase(Locale.ROOT);
        }

        public Interaction interaction() {
            return interaction;
        }

        @Override
        public String toString() {
            return label;
        }
    }

    private static final int MAX_NODE_LENGTH = 1024;

    private static final String CONTEXT_PREFIX = "kibana:";
    private static final String CONTEXT_MARKER = ";" + CONTEXT_PREFIX;
    private static final char NODE_SEPARATOR = ';';
    private static final char PART_SEPARATOR = ':';

    private static final String APPLICATION = "application:";
    private static final String DASHBOARD = "dashboard:";
    private static final String TASK_MANAGER = "task manager:";
    private static final String TASK_RUN = "run ";
    private static final String ALERT_EXECUTE = "alert:execute ";

    private static final Map<String, Feature> APPS = Map.of(
        "discover",
        Feature.DISCOVER,
        "dashboards",
        Feature.DASHBOARDS,
        "lens",
        Feature.LENS,
        "maps",
        Feature.MAPS,
        "canvas",
        Feature.CANVAS,
        "dev_tools",
        Feature.DEV_TOOLS
    );

    private static final String RULE_TASK = "alerting:";
    private static final String BACKFILL_TASK = "ad_hoc_run-backfill";
    private static final String ALERTING_V2_TASK = "alerting_v2:";
    private static final String ENTITY_STORE_TASK = "entity_store:";
    private static final String REPORTING_TASK = "report:";

    private static final List<String> SECURITY_RULE_PREFIXES = List.of("siem.", "attack-discovery");
    private static final List<String> OBSERVABILITY_RULE_PREFIXES = List.of(
        "observability.",
        "slo.",
        "logs.",
        "metrics.",
        "apm.",
        "xpack.uptime.",
        "xpack.synthetics.",
        "datasetQuality.",
        "streams."
    );

    private KibanaExecutionContextClassifier() {}

    /**
     * Classifies the {@code X-Opaque-Id} header of a request that {@link QueryCallerClassifier} found to come from Kibana.
     * Returns {@link Feature#OTHER_KIBANA} when the header is absent or carries no recognised execution context.
     */
    public static Feature classify(@Nullable String opaqueId) {
        if (opaqueId == null) {
            return Feature.OTHER_KIBANA;
        }
        int rootStart = contextStart(opaqueId);
        if (rootStart < 0) {
            return Feature.OTHER_KIBANA;
        }
        int rootEnd = nodeEnd(opaqueId, rootStart);
        String root = decodeAscii(opaqueId, rootStart, rootEnd);
        if (root.startsWith(TASK_MANAGER)) {
            return classifyTask(root.substring(TASK_MANAGER.length()), opaqueId, rootEnd);
        }
        if (root.startsWith(ALERT_EXECUTE)) {
            Feature rule = ruleFamily(root, ALERT_EXECUTE.length());
            return rule == null ? Feature.OTHER_KIBANA : rule;
        }
        String app = null;
        if (root.startsWith(APPLICATION)) {
            app = part(root, APPLICATION.length());
        } else if (root.startsWith(DASHBOARD)) {
            app = part(root, DASHBOARD.length());
        }
        return app == null ? Feature.OTHER_KIBANA : APPS.getOrDefault(app, Feature.OTHER_KIBANA);
    }

    /**
     * The start of the root node. A request id taken from the caller's own {@code X-Opaque-Id} can itself contain the marker, and
     * Kibana encodes the context's parts, so the real context starts after the last marker.
     */
    private static int contextStart(String opaqueId) {
        int marker = opaqueId.lastIndexOf(CONTEXT_MARKER);
        if (marker >= 0) {
            return marker + CONTEXT_MARKER.length();
        }
        return opaqueId.startsWith(CONTEXT_PREFIX) ? CONTEXT_PREFIX.length() : -1;
    }

    private static int nodeEnd(String opaqueId, int start) {
        int end = opaqueId.indexOf(NODE_SEPARATOR, start);
        return end < 0 ? opaqueId.length() : end;
    }

    private static Feature classifyTask(String task, String opaqueId, int rootEnd) {
        if (task.startsWith(TASK_RUN) == false) {
            return Feature.OTHER_TASK_MANAGER;
        }
        String taskType = task.substring(TASK_RUN.length());
        if (taskType.startsWith(ALERTING_V2_TASK)) {
            return Feature.ALERTING_V2;
        }
        if (taskType.startsWith(ENTITY_STORE_TASK)) {
            return Feature.ENTITY_STORE;
        }
        if (taskType.startsWith(REPORTING_TASK)) {
            return Feature.REPORTING;
        }
        Feature rule = null;
        if (taskType.startsWith(RULE_TASK)) {
            rule = ruleFamily(taskType, RULE_TASK.length());
        } else if (taskType.startsWith(BACKFILL_TASK) && rootEnd < opaqueId.length()) {
            String child = decodeAscii(opaqueId, rootEnd + 1, nodeEnd(opaqueId, rootEnd + 1));
            rule = child.startsWith(ALERT_EXECUTE) ? ruleFamily(child, ALERT_EXECUTE.length()) : null;
        }
        return rule == null ? Feature.OTHER_TASK_MANAGER : rule;
    }

    /**
     * The rule family of the rule type id starting at {@code start}, or {@code null} if the id is empty.
     */
    @Nullable
    private static Feature ruleFamily(String node, int start) {
        String ruleType = part(node, start);
        if (ruleType.isEmpty()) {
            return null;
        }
        if (startsWithAny(ruleType, SECURITY_RULE_PREFIXES)) {
            return Feature.SECURITY_RULE;
        }
        if (startsWithAny(ruleType, OBSERVABILITY_RULE_PREFIXES)) {
            return Feature.OBSERVABILITY_RULE;
        }
        return Feature.STACK_RULE;
    }

    private static boolean startsWithAny(String value, List<String> prefixes) {
        for (String prefix : prefixes) {
            if (value.startsWith(prefix)) {
                return true;
            }
        }
        return false;
    }

    /**
     * The text from {@code start} to the next part separator or the end of {@code node}.
     */
    private static String part(String node, int start) {
        int end = node.indexOf(PART_SEPARATOR, start);
        return node.substring(start, end < 0 ? node.length() : end);
    }

    /**
     * Decodes {@code %XX} escapes of ASCII characters in at most {@link #MAX_NODE_LENGTH} characters of {@code s} from
     * {@code start}. Every name this class matches is ASCII, so other escapes, malformed ones and escapes cut off by Kibana's
     * length limit are kept as they are rather than rejected.
     */
    private static String decodeAscii(String s, int start, int end) {
        int limit = Math.min(end, start + MAX_NODE_LENGTH);
        StringBuilder decoded = new StringBuilder(limit - start);
        int i = start;
        while (i < limit) {
            char c = s.charAt(i);
            if (c == '%' && i + 2 < limit) {
                int hi = hexValue(s.charAt(i + 1));
                int lo = hexValue(s.charAt(i + 2));
                if (hi >= 0 && hi < 8 && lo >= 0) {
                    decoded.append((char) ((hi << 4) | lo));
                    i += 3;
                    continue;
                }
            }
            decoded.append(c);
            i++;
        }
        return decoded.toString();
    }

    private static int hexValue(char c) {
        if (c >= '0' && c <= '9') {
            return c - '0';
        }
        if (c >= 'A' && c <= 'F') {
            return c - 'A' + 10;
        }
        if (c >= 'a' && c <= 'f') {
            return c - 'a' + 10;
        }
        return -1;
    }
}
