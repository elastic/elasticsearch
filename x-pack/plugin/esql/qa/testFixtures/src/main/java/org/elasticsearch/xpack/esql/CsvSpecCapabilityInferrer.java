/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

import org.elasticsearch.logging.LogManager;
import org.elasticsearch.logging.Logger;
import org.elasticsearch.xpack.esql.CsvSpecReader.CsvTestCase;
import org.elasticsearch.xpack.esql.action.EsqlCapabilities;

import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static org.elasticsearch.xpack.esql.CsvTestsDataLoader.ENRICH_POLICIES;

/**
 * Automatically infers required ES|QL capabilities from a csv-spec test case's query text and
 * enrich policy configuration, then merges them with any explicit {@code required_capability:}
 * directives already present in the test case.
 *
 * <p>The motivation is to prevent BWC test failures caused by missing capability gates.
 * Authors routinely add the fix/behaviour capability that is the subject of their test (e.g.
 * {@code fix_case_partial_fold_keyword_type}) but forget to also declare the function or type
 * capabilities needed by the query (e.g. {@code fn_to_text} or
 * {@code date_range_field_type_v6}).  In mixed-version clusters the old nodes then look
 * eligible, run the query, and either crash or return unexpected results.</p>
 *
 * <h2>What is inferred</h2>
 * <ul>
 *   <li><strong>Function capabilities</strong> ({@code fn_&lt;name&gt;}): every function call
 *       found in the query (by regex) is mapped to its {@code fn_} capability if one exists in
 *       the full registry.  Keywords such as {@code FROM}, {@code WHERE}, {@code STATS} etc.
 *       are silently ignored because they have no corresponding {@code fn_*} capability.</li>
 *   <li><strong>Enrich date-range capabilities</strong>: when a query issues
 *       {@code ENRICH &lt;policy&gt;} and that policy is backed by an index whose match field
 *       has type {@code date_range}, {@link EsqlCapabilities.Cap#DATE_RANGE_FIELD_TYPE_V6} is
 *       added automatically.  This covers the exact failure mode of the enrichDecadesStats BWC
 *       regression.</li>
 * </ul>
 *
 * <h2>What is NOT inferred</h2>
 * <p>Fix/behaviour capabilities (e.g. {@code fix_case_partial_fold_keyword_type}) must still be
 * declared explicitly — there is no way to know the <em>intent</em> of a test from its query
 * text alone.  Coordinator-only ({@code required_capability_coordinator:}) and
 * negative-capability ({@code missing_capability_*}) directives also remain manual.</p>
 *
 * <h2>Safety</h2>
 * <p>Only capabilities that are present in the full capability registry are ever added (false
 * positives from regex matching are filtered out).  Inference failures (missing resource files,
 * JSON parse errors) produce no inferred capabilities rather than throwing; the test continues
 * with its explicit gates only.</p>
 */
public final class CsvSpecCapabilityInferrer {

    private static final Logger LOGGER = LogManager.getLogger(CsvSpecCapabilityInferrer.class);

    /**
     * Shared instance.  Initialised once from the full capability set (including snapshot-only
     * and feature-flag-gated capabilities) so that BWC inference works regardless of the
     * current build flavour.
     */
    public static final CsvSpecCapabilityInferrer INSTANCE = new CsvSpecCapabilityInferrer(
        EsqlCapabilities.capabilities(EsqlTestUtils.TEST_FUNCTION_REGISTRY, true)
    );

    /**
     * Matches any identifier immediately followed by {@code (} — the ES|QL syntax for a
     * function call.  Non-function keywords (FROM, WHERE, …) are filtered downstream because
     * they have no {@code fn_*} capability in the registry.
     */
    private static final Pattern FUNCTION_CALL = Pattern.compile("\\b([A-Za-z_][A-Za-z0-9_]*)\\s*\\(", Pattern.MULTILINE);

    /**
     * Matches an ENRICH command, capturing the policy name.  Handles the optional mode prefix
     * ({@code _coordinator:}, {@code _remote:}, {@code _any:}, etc.) which starts with
     * {@code _}.
     *
     * <pre>ENRICH [_mode:]policy_name ON ...</pre>
     */
    private static final Pattern ENRICH_COMMAND = Pattern.compile(
        "\\bENRICH\\s+(?:_\\w+:)?([A-Za-z_][A-Za-z0-9_]*)",
        Pattern.CASE_INSENSITIVE
    );

    private static final ObjectMapper JSON = new ObjectMapper();

    /** Subset of allKnownCaps that start with {@code fn_}. */
    private final Set<String> functionCaps;
    /** Full set of known capabilities, used for membership checks on enrich policy caps. */
    private final Set<String> allKnownCaps;
    /**
     * Mapping from lowercase policy name to the capabilities its index type requires.
     * Computed once at class load from the enrich policy resource files.
     */
    private static final Map<String, List<String>> POLICY_CAPS = buildPolicyCaps();

    public CsvSpecCapabilityInferrer(EsqlCapabilities allCaps) {
        Set<String> fnCaps = new HashSet<>();
        for (String cap : allCaps.capabilities()) {
            if (cap.startsWith("fn_")) {
                fnCaps.add(cap);
            }
        }
        this.functionCaps = Collections.unmodifiableSet(fnCaps);
        this.allKnownCaps = allCaps.capabilities();
    }

    /**
     * Merges inferred capabilities into {@code testCase.requiredCapabilities}.
     *
     * <p>Inferred capabilities that are already explicitly listed, or that are absent from the
     * registry, are silently skipped.  If there is nothing to infer the list is unchanged.</p>
     *
     * <p>Only {@code requiredCapabilities} (all-cluster check) is augmented.
     * {@code requiredCapabilitiesLocalCluster}, {@code missingCapabilities*}, etc. are left
     * unchanged because they carry coordinator-specific or inverse semantics that cannot be
     * derived from the query text.</p>
     */
    public void augmentRequiredCapabilities(CsvTestCase testCase) {
        if (testCase.query == null || testCase.query.isBlank()) {
            return;
        }
        Set<String> explicit = new HashSet<>(testCase.requiredCapabilities);
        Set<String> inferred = inferFromQuery(testCase.query);
        inferred.removeAll(explicit);
        if (inferred.isEmpty() == false) {
            List<String> merged = new ArrayList<>(testCase.requiredCapabilities);
            merged.addAll(inferred);
            testCase.requiredCapabilities = Collections.unmodifiableList(merged);
        }
    }

    private Set<String> inferFromQuery(String query) {
        String stripped = stripLiteralsAndComments(query);
        Set<String> result = new HashSet<>();

        // Function capabilities: fn_<name>
        Matcher m = FUNCTION_CALL.matcher(stripped);
        while (m.find()) {
            String cap = "fn_" + m.group(1).toLowerCase(Locale.ROOT);
            if (functionCaps.contains(cap)) {
                result.add(cap);
            }
        }

        // Enrich policy capabilities
        m = ENRICH_COMMAND.matcher(stripped);
        while (m.find()) {
            String policyName = m.group(1).toLowerCase(Locale.ROOT);
            List<String> policyCaps = POLICY_CAPS.get(policyName);
            if (policyCaps != null) {
                for (String cap : policyCaps) {
                    if (allKnownCaps.contains(cap)) {
                        result.add(cap);
                    }
                }
            }
        }

        return result;
    }

    /**
     * Returns a copy of {@code query} with string literals and line comments removed,
     * replaced by a single space to preserve word boundaries.
     *
     * <p>ES|QL supports two string literal forms:
     * <ul>
     *   <li>Triple-quoted: {@code """..."""} — no escape sequences inside.</li>
     *   <li>Double-quoted: {@code "..."} — {@code \"} and {@code \\} are escape sequences.</li>
     * </ul>
     * <p>Line comments ({@code // ...}) are also stripped.  This prevents regex matches on
     * function-like tokens that appear inside literal values rather than in the query syntax
     * itself (e.g. {@code WHERE wkt = "POLYGON((0 0, 1 0))"} must not infer {@code fn_polygon}).
     */
    static String stripLiteralsAndComments(String query) {
        StringBuilder result = new StringBuilder(query.length());
        int i = 0;
        int len = query.length();
        while (i < len) {
            char c = query.charAt(i);

            // Line comment: // to end of line
            if (c == '/' && i + 1 < len && query.charAt(i + 1) == '/') {
                while (i < len && query.charAt(i) != '\n') {
                    i++;
                }
                result.append(' ');
                continue;
            }

            // Triple-quoted string: """...""" (no escape sequences inside)
            if (c == '"' && i + 2 < len && query.charAt(i + 1) == '"' && query.charAt(i + 2) == '"') {
                i += 3;
                while (i + 2 < len) {
                    if (query.charAt(i) == '"' && query.charAt(i + 1) == '"' && query.charAt(i + 2) == '"') {
                        i += 3;
                        break;
                    }
                    i++;
                }
                result.append(' ');
                continue;
            }

            // Regular double-quoted string: "..." with \" and \\ escapes
            if (c == '"') {
                i++; // skip opening "
                while (i < len) {
                    char sc = query.charAt(i);
                    if (sc == '\\' && i + 1 < len) {
                        i += 2; // skip escape sequence
                    } else if (sc == '"') {
                        i++; // skip closing "
                        break;
                    } else {
                        i++;
                    }
                }
                result.append(' ');
                continue;
            }

            result.append(c);
            i++;
        }
        return result.toString();
    }

    // -----------------------------------------------------------------------
    // Static helpers for POLICY_CAPS initialisation
    // -----------------------------------------------------------------------

    private static Map<String, List<String>> buildPolicyCaps() {
        Map<String, List<String>> result = new HashMap<>();
        for (Map.Entry<String, CsvTestsDataLoader.EnrichConfig> entry : ENRICH_POLICIES.entrySet()) {
            List<String> caps = capsForPolicy(entry.getValue());
            if (caps.isEmpty() == false) {
                result.put(entry.getKey().toLowerCase(Locale.ROOT), caps);
            }
        }
        return Collections.unmodifiableMap(result);
    }

    /**
     * Determines which capabilities are required by the given enrich policy based on its
     * source index field types.
     *
     * <p>Currently only {@code date_range} match fields are handled (→
     * {@link EsqlCapabilities.Cap#DATE_RANGE_FIELD_TYPE_V6}).  Other range sub-types
     * ({@code integer_range}, {@code double_range}, …) have always been supported and require
     * no additional capability gate.</p>
     */
    private static List<String> capsForPolicy(CsvTestsDataLoader.EnrichConfig config) {
        try {
            String policyJson = config.loadPolicy();
            JsonNode root = JSON.readTree(policyJson);
            JsonNode rangeNode = root.get("range");
            if (rangeNode == null) {
                return List.of(); // match / geo_match policy — no type capability needed
            }
            JsonNode matchFieldNode = rangeNode.get("match_field");
            if (matchFieldNode == null) {
                return List.of();
            }
            String matchField = matchFieldNode.asText();
            String fieldType = loadFieldType(config.index(), matchField);
            if ("date_range".equals(fieldType)) {
                return List.of(EsqlCapabilities.Cap.DATE_RANGE_FIELD_TYPE_V6.capabilityName());
            }
            return List.of();
        } catch (Exception e) {
            LOGGER.warn("Failed to infer capabilities for enrich policy [{}]; skipping inference for this policy", config, e);
            return List.of();
        }
    }

    /**
     * Loads the type of {@code fieldName} from the standard index mapping resource
     * {@code /index/mappings/mapping-&lt;indexName&gt;.json}.
     *
     * @return the ES field type string (e.g. {@code "date_range"}), or {@code null} if the
     *         resource or field is not found
     */
    private static String loadFieldType(String indexName, String fieldName) throws Exception {
        String resourcePath = "/index/mappings/mapping-" + indexName + ".json";
        InputStream stream = CsvSpecCapabilityInferrer.class.getResourceAsStream(resourcePath);
        if (stream == null) {
            return null;
        }
        String json = new String(stream.readAllBytes(), StandardCharsets.UTF_8);
        JsonNode root = JSON.readTree(json);
        JsonNode properties = root.get("properties");
        if (properties == null) {
            return null;
        }
        JsonNode field = properties.get(fieldName);
        if (field == null) {
            return null;
        }
        JsonNode type = field.get("type");
        return type != null ? type.asText() : null;
    }
}
