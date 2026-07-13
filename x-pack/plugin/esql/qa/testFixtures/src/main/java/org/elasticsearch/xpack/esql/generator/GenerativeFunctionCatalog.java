/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.generator;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

import org.elasticsearch.Build;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.net.JarURLConnection;
import java.net.URL;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.jar.JarFile;

/**
 * Registry of ES|QL scalar function definitions, loaded at test time from the generated JSON docs
 * packaged under {@code esql-functions/} on the test classpath.
 * <p>
 * Used by {@link org.elasticsearch.xpack.esql.generator.function.CompositeFunctionGenerator}
 * to select type-compatible functions when building nested expressions.
 */
public final class GenerativeFunctionCatalog {

    /**
     * Parameter types we cannot synthesize expressions for in the generator,
     * so any signature requiring them is excluded.
     */
    private static final Set<String> UNSUPPORTED_PARAM_TYPES = Set.of(
        "date_period",
        "time_duration",
        "geo_point",
        "cartesian_point",
        "geo_shape",
        "cartesian_shape",
        "source",
        "tdigest",
        // Counter types cannot appear in EVAL output; exclude any function signatures that take them as params
        "counter_long",
        "counter_double",
        "counter_integer",
        // Dense vectors from different columns may have different dimensions; functions like
        // v_dot_product require matching dimensions which the generator cannot verify at build time.
        "dense_vector"
    );

    /**
     * Functions excluded from composite generation because they have constraints that cannot
     * be satisfied by arbitrary recursive expressions AND are not handled by a
     * {@link org.elasticsearch.xpack.esql.generator.function.SpecialFunctionGenerator}.
     * <p>
     * Before adding a function here, consider whether a {@link
     * org.elasticsearch.xpack.esql.generator.function.SpecialFunctionGeneratorRegistry}
     * entry would let it participate in composite expressions instead.
     */
    static final Set<String> EXCLUDED_FUNCTIONS = Set.of(
        // Full-text search functions (FullTextFunction subclasses) are only supported in WHERE and
        // STATS commands, or in EVAL within score(.) — not as general EVAL expressions.
        "match",
        "match_phrase",
        "kql",
        "qstr",
        "term",
        "terms_query",
        "knn",
        "score",
        // Aggregate-context functions
        "bucket",
        "categorize",
        // Functions that enforce isFoldable() on a required param with no clean literal substitute
        "mv_pseries_weighted_sum",
        "trange",
        // top_snippets(field, query): query must be a constant — but it must also be a valid
        // full-text search query string tied to the field's analyzer. Hard to synthesise safely.
        "top_snippets",
        // Functions whose required input must be a specific constant string value that cannot
        // be produced generically (time duration literals, inference model IDs, etc.)
        "to_timeduration",
        "embedding",
        "text_embedding",
        // from_base64 throws "Last unit does not have enough valid bits" instead of returning null
        // on non-base64 input — a real ES|QL bug (should return null gracefully); exclude until fixed.
        "from_base64",
        // Numeric clamping functions: handled by MathFunctionGenerator with typed literal bounds;
        // their keyword overloads are undertested and may trigger server-side bugs.
        "clamp",
        "clamp_min",
        "clamp_max"
    );

    private static final String RESOURCE_DIR = "esql-functions";

    private static final GenerativeFunctionCatalog INSTANCE = new GenerativeFunctionCatalog();

    /** All usable scalar functions indexed by the return type they can produce. */
    private final Map<String, List<GenerativeFunctionDefinition>> byReturnType;

    private GenerativeFunctionCatalog() {
        List<GenerativeFunctionDefinition> all = loadAll();
        Map<String, List<GenerativeFunctionDefinition>> index = new HashMap<>();
        for (GenerativeFunctionDefinition fn : all) {
            for (GenerativeFunctionSignature sig : fn.signatures()) {
                index.computeIfAbsent(sig.returnType(), t -> new ArrayList<>()).add(fn);
            }
        }
        // Deduplicate: a function with multiple signatures returning the same type would appear more than once
        Map<String, List<GenerativeFunctionDefinition>> deduped = new HashMap<>();
        for (Map.Entry<String, List<GenerativeFunctionDefinition>> e : index.entrySet()) {
            deduped.put(e.getKey(), e.getValue().stream().distinct().toList());
        }
        this.byReturnType = Collections.unmodifiableMap(deduped);
    }

    public static GenerativeFunctionCatalog getInstance() {
        return INSTANCE;
    }

    /**
     * Returns the set of ES|QL types that are interchangeable with {@code type} in the generator.
     * <p>
     * Two equivalences are recognised:
     * <ul>
     *   <li>{@code date} ↔ {@code datetime}: the Kibana function JSON definitions use {@code date}
     *       while the generator's column schema uses {@code datetime}.</li>
     *   <li>{@code text} ↔ {@code keyword}: ES|QL implicitly coerces between these in most
     *       expression contexts.</li>
     * </ul>
     */
    public static Set<String> equivalentTypes(String type) {
        return switch (type) {
            case "date", "datetime" -> Set.of("date", "datetime");
            // text columns are acceptable wherever keyword is expected, but not the reverse:
            // some functions specifically accept text and reject keyword.
            case "keyword" -> Set.of("keyword", "text");
            default -> Set.of(type);
        };
    }

    /**
     * Returns all usable scalar functions that have at least one signature returning {@code type}
     * or any equivalent type (see {@link #equivalentTypes}).
     */
    public List<GenerativeFunctionDefinition> scalarsReturning(String type) {
        return equivalentTypes(type).stream().flatMap(t -> byReturnType.getOrDefault(t, List.of()).stream()).distinct().toList();
    }

    /**
     * Returns the signatures of {@code fn} that can be satisfied given {@code columnTypes}.
     * A signature is considered satisfiable if, for every required parameter, its type is
     * either present in {@code columnTypes} (or an equivalent type) or producible by some
     * function in this catalog.
     */
    public List<GenerativeFunctionSignature> satisfiableSignatures(GenerativeFunctionDefinition fn, Set<String> columnTypes) {
        List<GenerativeFunctionSignature> result = new ArrayList<>();
        for (GenerativeFunctionSignature sig : fn.signatures()) {
            boolean satisfiable = true;
            for (GenerativeFunctionParam param : sig.params()) {
                if (param.optional()) {
                    continue;
                }
                if (isProducible(param.type(), columnTypes) == false) {
                    satisfiable = false;
                    break;
                }
            }
            if (satisfiable) {
                result.add(sig);
            }
        }
        return result;
    }

    /**
     * Returns {@code true} if an expression of {@code type} can be produced — either directly
     * from a column (including equivalent types) or by calling a function in this catalog.
     */
    private boolean isProducible(String type, Set<String> columnTypes) {
        return equivalentTypes(type).stream().anyMatch(t -> columnTypes.contains(t) || byReturnType.containsKey(t));
    }

    // ---- loading ----

    private static List<GenerativeFunctionDefinition> loadAll() {
        boolean isSnapshot = Build.current().isSnapshot();
        ObjectMapper mapper = new ObjectMapper();
        List<GenerativeFunctionDefinition> result = new ArrayList<>();

        URL dirUrl = GenerativeFunctionCatalog.class.getClassLoader().getResource(RESOURCE_DIR);
        if (dirUrl == null) {
            throw new IllegalStateException(
                "Resource directory '"
                    + RESOURCE_DIR
                    + "' not found on classpath. "
                    + "Ensure the processResources block in testFixtures/build.gradle copies the function JSONs."
            );
        }

        try {
            String protocol = dirUrl.getProtocol();
            if ("file".equals(protocol)) {
                loadFromDirectory(new File(dirUrl.toURI()), mapper, isSnapshot, result);
            } else if ("jar".equals(protocol)) {
                loadFromJar(dirUrl, mapper, isSnapshot, result);
            } else {
                throw new IllegalStateException("Unsupported resource protocol: " + protocol);
            }
        } catch (Exception e) {
            throw new RuntimeException("Failed to load ES|QL function definitions from '" + RESOURCE_DIR + "'", e);
        }
        return result;
    }

    private static void loadFromDirectory(File dir, ObjectMapper mapper, boolean isSnapshot, List<GenerativeFunctionDefinition> result)
        throws IOException {
        File[] files = dir.listFiles(f -> f.getName().endsWith(".json"));
        if (files == null) {
            return;
        }
        for (File f : files) {
            GenerativeFunctionDefinition def = parse(mapper, mapper.readTree(f));
            if (shouldInclude(def, isSnapshot)) {
                result.add(def);
            }
        }
    }

    private static void loadFromJar(URL dirUrl, ObjectMapper mapper, boolean isSnapshot, List<GenerativeFunctionDefinition> result)
        throws IOException {
        JarURLConnection conn = (JarURLConnection) dirUrl.openConnection();
        try (JarFile jar = conn.getJarFile()) {
            jar.stream().filter(e -> e.getName().startsWith(RESOURCE_DIR + "/") && e.getName().endsWith(".json")).forEach(e -> {
                try (InputStream is = jar.getInputStream(e)) {
                    GenerativeFunctionDefinition def = parse(mapper, mapper.readTree(is));
                    if (shouldInclude(def, isSnapshot)) {
                        result.add(def);
                    }
                } catch (IOException ex) {
                    throw new RuntimeException(ex);
                }
            });
        }
    }

    private static boolean shouldInclude(GenerativeFunctionDefinition def, boolean isSnapshot) {
        if ("scalar".equals(def.functionType()) == false) {
            return false;
        }
        if (def.snapshotOnly() && isSnapshot == false) {
            return false;
        }
        if (EXCLUDED_FUNCTIONS.contains(def.name())) {
            return false;
        }
        // Keep only functions that have at least one signature with no unsupported required param types
        return def.signatures()
            .stream()
            .anyMatch(sig -> sig.params().stream().noneMatch(p -> p.optional() == false && UNSUPPORTED_PARAM_TYPES.contains(p.type())));
    }

    private static GenerativeFunctionDefinition parse(ObjectMapper mapper, JsonNode root) {
        String name = root.get("name").asText();
        String functionType = root.get("type").asText();
        boolean snapshotOnly = root.path("snapshot_only").asBoolean(false);
        boolean preview = root.path("preview").asBoolean(false);

        List<GenerativeFunctionSignature> signatures = new ArrayList<>();
        for (JsonNode sigNode : root.get("signatures")) {
            String returnType = sigNode.get("returnType").asText();
            boolean variadic = sigNode.path("variadic").asBoolean(false);
            List<GenerativeFunctionParam> params = new ArrayList<>();
            for (JsonNode paramNode : sigNode.get("params")) {
                params.add(
                    new GenerativeFunctionParam(
                        paramNode.get("name").asText(),
                        paramNode.get("type").asText(),
                        paramNode.path("optional").asBoolean(false)
                    )
                );
            }
            signatures.add(new GenerativeFunctionSignature(List.copyOf(params), returnType, variadic));
        }
        return new GenerativeFunctionDefinition(name, functionType, List.copyOf(signatures), snapshotOnly, preview);
    }
}
