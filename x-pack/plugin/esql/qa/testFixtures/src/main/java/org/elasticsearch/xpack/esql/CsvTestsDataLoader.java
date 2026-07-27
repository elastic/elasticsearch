/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.esql;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;

import org.apache.http.HttpEntity;
import org.apache.http.HttpHost;
import org.apache.http.auth.AuthScope;
import org.apache.http.auth.UsernamePasswordCredentials;
import org.apache.http.client.CredentialsProvider;
import org.apache.http.impl.client.BasicCredentialsProvider;
import org.elasticsearch.client.Request;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.Response;
import org.elasticsearch.client.ResponseException;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestClientBuilder;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.logging.LogConfigurator;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.index.SliceIndexing;
import org.elasticsearch.inference.TaskType;
import org.elasticsearch.logging.LogManager;
import org.elasticsearch.logging.Logger;
import org.elasticsearch.test.rest.ESRestTestCase;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentFactory;
import org.elasticsearch.xcontent.XContentType;
import org.elasticsearch.xcontent.yaml.YamlXContent;
import org.elasticsearch.xpack.esql.action.EsqlCapabilities;
import org.elasticsearch.xpack.esql.core.type.EsField;
import org.elasticsearch.xpack.esql.view.RestPutViewAction;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.UncheckedIOException;
import java.net.URI;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.TreeMap;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.stream.Stream;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static org.elasticsearch.common.logging.LoggerMessageFormat.format;
import static org.elasticsearch.xpack.esql.CsvTestUtils.COMMA_ESCAPING_REGEX;
import static org.elasticsearch.xpack.esql.CsvTestUtils.ESCAPED_COMMA_SEQUENCE;
import static org.elasticsearch.xpack.esql.CsvTestUtils.multiValuesAwareCsvToStringArray;
import static java.util.stream.Collectors.toMap;
import static org.elasticsearch.xpack.esql.EsqlTestUtils.reader;

public class CsvTestsDataLoader {

    static {
        // Ensure the logging factory is initialized before the static logger field below. When running standalone (via main() or
        // Gradle's loadCsvSpecData task), nothing has initialized the ES logging system before this class is loaded.
        LogConfigurator.configureESLogging();
    }

    private static final Logger logger = LogManager.getLogger(CsvTestsDataLoader.class);

    private static final int BULK_DATA_SIZE = 100_000;

    private static final RequestOptions DEPRECATED_DEFAULT_METRIC_WARNING_HANDLER = RequestOptions.DEFAULT.toBuilder()
        .setWarningsHandler(
            warnings -> warnings.stream()
                .anyMatch(
                    warning -> "Parameter [default_metric] is deprecated and will be removed in a future version".equals(warning) == false
                )
        )
        .build();

    /**
     * All test data definitions (indices, enrich policies, inference endpoints and views) are loaded from the
     * {@code spec_data.yml} manifest resource, which is the source of truth. Edit that file to add or change data.
     * The {@code --dump-manifest} developer tool in {@link #main} can regenerate the manifest if needed.
     */
    private static final Map<String, Object> MANIFEST = readManifest();

    public static final Map<String, TestDataset> CSV_DATASET = parseManifestDatasets(MANIFEST);

    // Developer flags for faster iteration when debugging specific csv-spec tests:
    // -Dtests.spec_indices=index1,index2 load only the specified dataset indices (enrich skipped unless spec_enrich_policies is set)
    // -Dtests.spec_enrich_policies=p1,p2 load only the specified enrich policies (overrides the spec_indices skipping of enrich)
    @Nullable
    private static final Set<String> specIndices = parseSetProperty("tests.spec_indices");
    @Nullable
    private static final Set<String> specEnrichPolicies = parseSetProperty("tests.spec_enrich_policies");

    public static final Map<String, EnrichConfig> ENRICH_POLICIES = parseManifestEnrich(MANIFEST);

    public static final Map<String, InferenceConfig> INFERENCE_CONFIGS = parseManifestInference(MANIFEST);

    public static final Map<String, ViewConfig> VIEW_CONFIGS = parseManifestViews(MANIFEST);

    /**
     * Categories group csv-spec files by the data they need, so a test suite can load only that data. Each
     * category loads a fixed set of indices (or all of them) plus enrich policies, and either loads views or
     * not. This is what lets some {@code FROM *} tests see views while others do not: view presence is decided
     * by the file's category, never by the wildcard resolution itself.
     */
    public static final Map<String, Category> CATEGORIES = parseManifestCategories(MANIFEST);

    /** Maps a csv-spec file (its group name, i.e. file name without the {@code .csv-spec} extension) to a category name. */
    public static final Map<String, String> FILE_CATEGORY = parseManifestFiles(MANIFEST);

    /** Returns the category a csv-spec file belongs to. Accepts either {@code "stats"} or {@code "stats.csv-spec"}. */
    public static Category categoryFor(String specFileNameOrGroup) {
        String group = specFileNameOrGroup.endsWith(".csv-spec")
            ? specFileNameOrGroup.substring(0, specFileNameOrGroup.length() - ".csv-spec".length())
            : specFileNameOrGroup;
        String categoryName = FILE_CATEGORY.get(group);
        if (categoryName == null) {
            throw new IllegalArgumentException("No category mapping for csv-spec file [" + group + "] in " + MANIFEST_RESOURCE);
        }
        return dataForCategory(categoryName);
    }

    /** Returns the definition of a named category. */
    public static Category dataForCategory(String name) {
        Category category = CATEGORIES.get(name);
        if (category == null) {
            throw new IllegalStateException("Category [" + name + "] is not defined in " + MANIFEST_RESOURCE);
        }
        return category;
    }

    /**
     * Index aliases created unconditionally alongside the main test indices. These are not tied
     * to view support — any csv-spec test may reference them. Non-view tests that use wildcard
     * patterns (e.g. {@code FROM employees*}) are unaffected because Elasticsearch field-caps
     * deduplicates an alias and its backing index into a single logical source.
     */
    public static final Map<String, AliasConfig> ALIAS_CONFIGS = Stream.of(new AliasConfig("employees_alias", "employees"))
        .collect(toMap(AliasConfig::aliasName, Function.identity()));

    /**
     * <p>
     * Loads spec data on a local ES server.
     * </p>
     * <p>
     * Accepts an URL as first argument, eg. http://localhost:9200 or http://user:pass@localhost:9200
     *</p>
     * <p>
     * If no arguments are specified, the default URL is http://localhost:9200 without authentication
     * </p>
     * <p>
     * It also supports HTTPS
     * </p>
     * @param args the URL to connect
     * @throws IOException
     */
    public static void main(String[] args) throws IOException {
        // Need to setup the log configuration properly to avoid messages when creating a new RestClient
        LogConfigurator.configureESLogging();

        // Developer tool: rewrite the spec-data manifest in the canonical, sorted format from the in-memory maps.
        // Usage: ... loadCsvSpecData --args="--dump-manifest /abs/path/spec_data.yml"
        // Since the maps are themselves loaded from the manifest, this is effectively a normalizer/formatter for
        // the indices/enrich/inference/views sections. It does NOT emit the hand-maintained categories/files
        // sections, so only run it against a scratch path and merge, never overwrite the checked-in manifest.
        if (args.length == 2 && "--dump-manifest".equals(args[0])) {
            dumpManifestDefinitions(Path.of(args[1]));
            return;
        }

        boolean indexes = false;
        boolean policies = false;
        boolean views = false;
        boolean delete = false;
        boolean load = false;

        RestClientBuilder builder = RestClient.builder(new HttpHost("localhost", 9200, "http"));
        for (String arg : args) {
            if (arg.startsWith("--")) {
                switch (arg.substring(2).toLowerCase(Locale.ROOT)) {
                    case "indexes", "indices", "data":
                        indexes = true;
                        break;
                    case "policies", "enrich":
                        policies = true;
                        break;
                    case "views":
                        views = true;
                        break;
                    case "delete":
                        delete = true;
                        break;
                    case "load":
                        load = true;
                        break;
                    default:
                        throw new IllegalArgumentException(
                            "unknown option: " + arg + " (valid options are: --indexes, --policies, --views, --delete, --load)"
                        );
                }
            } else {
                URL url = URI.create(args[0]).toURL();
                String protocol = url.getProtocol();
                String host = url.getHost();
                int port = url.getPort();
                if (port < 0 || port > 65535) {
                    throw new IllegalArgumentException("Please specify a valid port [0 - 65535], found [" + port + "]");
                }
                builder = RestClient.builder(new HttpHost(host, port, protocol));
                String userInfo = url.getUserInfo();
                if (userInfo != null) {
                    if (userInfo.contains(":") == false || userInfo.split(":").length != 2) {
                        throw new IllegalArgumentException("Invalid user credentials [username:password], found [" + userInfo + "]");
                    }
                    String[] userPw = userInfo.split(":");
                    String username = userPw[0];
                    String password = userPw[1];
                    CredentialsProvider credentialsProvider = new BasicCredentialsProvider();
                    credentialsProvider.setCredentials(AuthScope.ANY, new UsernamePasswordCredentials(username, password));
                    builder = builder.setHttpClientConfigCallback(
                        httpClientBuilder -> httpClientBuilder.setDefaultCredentialsProvider(credentialsProvider)
                    );
                }
            }
        }
        // Choose all if none specified
        if (indexes == false && policies == false && views == false) {
            indexes = true;
            policies = true;
            views = true;
        }
        // Delete and re-load if none specified
        if (delete == false && load == false) {
            delete = true;
            load = true;
        }

        try (RestClient client = builder.build()) {
            if (delete) {
                if (views) {
                    deleteViews(client);
                }
                if (policies) {
                    deleteEnrichPolicies(client);
                }
                if (indexes) {
                    deleteIndexes(client, true, true, false, false, cap -> true);
                }
            }
            if (load) {
                if (indexes) {
                    loadDataSets(client, true, true, false, false, cap -> true, (restClient, indexName, indexMapping, indexSettings) -> {
                        // don't use ESRestTestCase methods here or, if you do, test running the main method before making the change
                        StringBuilder jsonBody = new StringBuilder("{");
                        if (indexSettings != null && indexSettings.isEmpty() == false) {
                            jsonBody.append("\"settings\":");
                            jsonBody.append(Strings.toString(indexSettings));
                            jsonBody.append(",");
                        }
                        jsonBody.append("\"mappings\":");
                        jsonBody.append(indexMapping);
                        jsonBody.append("}");

                        Request request = new Request("PUT", "/" + indexName);
                        request.setJsonEntity(jsonBody.toString());
                        restClient.performRequest(request);
                    });
                }
                if (policies) {
                    loadEnrichPolicies(client);
                }
                if (indexes) {
                    loadAliasesIntoEs(client);
                }
                if (views) {
                    loadViewsIntoEs(client);
                }
            }
        }
    }

    /**
     * Serializes the (currently hardcoded) dataset/enrich/inference/view definitions to a YAML manifest.
     * Entries are sorted by name for a stable, reviewable diff. Every non-default field is emitted explicitly
     * so the output round-trips losslessly through the manifest reader.
     */
    static void dumpManifestDefinitions(Path path) throws IOException {
        try (OutputStream out = Files.newOutputStream(path); XContentBuilder b = XContentFactory.yamlBuilder(out)) {
            b.startObject();

            // "indices" = real Elasticsearch indices (the CSV_DATASET / TestDataset entries). This is distinct from
            // the external "dataset:" sources (parquet/csv/sql) declared inline in csv-spec files, which are
            // registered by the esql-datasource-* plugins and are intentionally NOT part of this manifest.
            b.startArray("indices");
            List<TestDataset> datasets = new ArrayList<>(CSV_DATASET.values());
            datasets.sort(Comparator.comparing(TestDataset::indexName));
            for (TestDataset d : datasets) {
                b.startObject();
                b.field("index", d.indexName());
                if (d.mappingFileName() != null) {
                    b.field("mapping", d.mappingFileName());
                }
                if (d.dataFileName() != null) {
                    b.field("data", d.dataFileName());
                }
                if (d.settingFileName() != null) {
                    b.field("settings", d.settingFileName());
                }
                if (d.allowSubFields() == false) {
                    b.field("subfields", false);
                }
                if (d.dynamic() != null) {
                    b.field("dynamic", d.dynamic());
                }
                writeStringMap(b, "type_mapping", d.typeMapping());
                writeStringMap(b, "dynamic_type_mapping", d.dynamicTypeMapping());
                if (d.inferenceEndpoints().isEmpty() == false) {
                    b.field("inference", d.inferenceEndpoints());
                }
                writeCapabilities(b, d.requiredCapabilities());
                b.endObject();
            }
            b.endArray();

            b.startArray("enrich");
            List<EnrichConfig> policies = new ArrayList<>(ENRICH_POLICIES.values());
            policies.sort(Comparator.comparing(EnrichConfig::policyName));
            for (EnrichConfig p : policies) {
                b.startObject();
                b.field("policy", p.policyName());
                b.field("file", p.policyFileName());
                b.field("index", p.index());
                b.endObject();
            }
            b.endArray();

            b.startArray("inference");
            List<InferenceConfig> inferences = new ArrayList<>(INFERENCE_CONFIGS.values());
            inferences.sort(Comparator.comparing(InferenceConfig::id));
            for (InferenceConfig i : inferences) {
                b.startObject();
                b.field("id", i.id());
                b.field("task_type", i.type().name());
                b.endObject();
            }
            b.endArray();

            b.startArray("views");
            List<ViewConfig> views = new ArrayList<>(VIEW_CONFIGS.values());
            views.sort(Comparator.comparing(ViewConfig::name));
            for (ViewConfig v : views) {
                b.startObject();
                b.field("name", v.name());
                writeCapabilities(b, v.requiredCapabilities());
                b.endObject();
            }
            b.endArray();

            b.endObject();
        }
        logger.info("Wrote spec-data manifest definitions to [{}]", path);
    }

    private static void writeStringMap(XContentBuilder b, String field, @Nullable Map<String, String> map) throws IOException {
        if (map == null || map.isEmpty()) {
            return;
        }
        b.startObject(field);
        // sort keys for a stable diff; a null value means "remove this field from the mapping"
        for (Map.Entry<String, String> e : new TreeMap<>(map).entrySet()) {
            if (e.getValue() == null) {
                b.nullField(e.getKey());
            } else {
                b.field(e.getKey(), e.getValue());
            }
        }
        b.endObject();
    }

    private static void writeCapabilities(XContentBuilder b, List<EsqlCapabilities.Cap> capabilities) throws IOException {
        if (capabilities.isEmpty()) {
            return;
        }
        List<String> names = new ArrayList<>(capabilities.size());
        for (EsqlCapabilities.Cap cap : capabilities) {
            names.add(cap.capabilityName());
        }
        b.field("capabilities", names);
    }

    static final String MANIFEST_RESOURCE = "/spec_data.yml";

    /** Reads the spec-data manifest resource into a nested map. */
    static Map<String, Object> readManifest() {
        try (InputStream in = getResourceStream(MANIFEST_RESOURCE)) {
            return XContentHelper.convertToMap(YamlXContent.yamlXContent, in, false);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    @SuppressWarnings("unchecked")
    static Map<String, TestDataset> parseManifestDatasets(Map<String, Object> root) {
        Map<String, TestDataset> out = new HashMap<>();
        for (Map<String, Object> m : (List<Map<String, Object>>) root.get("indices")) {
            String index = (String) m.get("index");
            boolean subfields = m.containsKey("subfields") == false || (Boolean) m.get("subfields");
            List<String> inference = m.containsKey("inference") ? (List<String>) m.get("inference") : List.of();
            out.put(
                index,
                new TestDataset(
                    index,
                    (String) m.get("mapping"),
                    (String) m.get("data"),
                    (String) m.get("settings"),
                    subfields,
                    parseStringMap(m.get("type_mapping")),
                    parseStringMap(m.get("dynamic_type_mapping")),
                    (String) m.get("dynamic"),
                    inference,
                    parseCapabilities(m.get("capabilities"))
                )
            );
        }
        return out;
    }

    @SuppressWarnings("unchecked")
    static Map<String, EnrichConfig> parseManifestEnrich(Map<String, Object> root) {
        Map<String, EnrichConfig> out = new HashMap<>();
        for (Map<String, Object> m : (List<Map<String, Object>>) root.get("enrich")) {
            out.put((String) m.get("policy"), new EnrichConfig((String) m.get("policy"), (String) m.get("file"), (String) m.get("index")));
        }
        return out;
    }

    @SuppressWarnings("unchecked")
    static Map<String, InferenceConfig> parseManifestInference(Map<String, Object> root) {
        Map<String, InferenceConfig> out = new HashMap<>();
        for (Map<String, Object> m : (List<Map<String, Object>>) root.get("inference")) {
            out.put((String) m.get("id"), new InferenceConfig((String) m.get("id"), TaskType.fromString((String) m.get("task_type"))));
        }
        return out;
    }

    @SuppressWarnings("unchecked")
    static Map<String, ViewConfig> parseManifestViews(Map<String, Object> root) {
        Map<String, ViewConfig> out = new HashMap<>();
        for (Map<String, Object> m : (List<Map<String, Object>>) root.get("views")) {
            out.put((String) m.get("name"), new ViewConfig((String) m.get("name"), parseCapabilities(m.get("capabilities"))));
        }
        return out;
    }

    @SuppressWarnings("unchecked")
    static Map<String, Category> parseManifestCategories(Map<String, Object> root) {
        Map<String, Category> out = new HashMap<>();
        for (Map.Entry<String, Object> e : ((Map<String, Object>) root.get("categories")).entrySet()) {
            Map<String, Object> m = (Map<String, Object>) e.getValue();
            List<String> indices = m.containsKey("indices") ? (List<String>) m.get("indices") : List.of();
            List<String> enrich = m.containsKey("enrich") ? (List<String>) m.get("enrich") : List.of();
            List<String> views = m.containsKey("views") ? (List<String>) m.get("views") : List.of();
            out.put(e.getKey(), new Category(e.getKey(), List.copyOf(indices), List.copyOf(enrich), List.copyOf(views)));
        }
        return out;
    }

    @SuppressWarnings("unchecked")
    static Map<String, String> parseManifestFiles(Map<String, Object> root) {
        return Map.copyOf((Map<String, String>) root.get("files"));
    }

    /** A null value means the field should be removed from the mapping (see {@link TestDataset#withTypeMapping}). */
    @SuppressWarnings("unchecked")
    @Nullable
    private static Map<String, String> parseStringMap(@Nullable Object value) {
        if (value == null) {
            return null;
        }
        Map<String, String> out = new HashMap<>();
        for (Map.Entry<String, Object> e : ((Map<String, Object>) value).entrySet()) {
            out.put(e.getKey(), (String) e.getValue());
        }
        return out;
    }

    @SuppressWarnings("unchecked")
    private static List<EsqlCapabilities.Cap> parseCapabilities(@Nullable Object value) {
        if (value == null) {
            return List.of();
        }
        List<EsqlCapabilities.Cap> out = new ArrayList<>();
        for (String name : (List<String>) value) {
            out.add(EsqlCapabilities.Cap.valueOf(name.toUpperCase(Locale.ROOT)));
        }
        return out;
    }

    public static Set<TestDataset> availableDatasetsForEs(
        boolean supportsIndexModeLookup,
        boolean supportsSourceFieldMapping,
        boolean inferenceEnabled,
        boolean requiresTimeSeries,
        Predicate<EsqlCapabilities.Cap> capabilityCheck
    ) throws IOException {
        Set<TestDataset> testDataSets = new HashSet<>();

        for (TestDataset dataset : CSV_DATASET.values()) {
            if ((inferenceEnabled || dataset.inferenceEndpoints().isEmpty())
                && (supportsIndexModeLookup || isLookupDataset(dataset) == false)
                && (supportsSourceFieldMapping || isSourceMappingDataset(dataset) == false)
                && (requiresTimeSeries == false || isTimeSeries(dataset))
                && dataset.requiredCapabilities.stream().allMatch(capabilityCheck)) {
                testDataSets.add(dataset);
            }
        }

        if (specIndices != null) {
            testDataSets.removeIf(d -> specIndices.contains(d.indexName) == false);
        }

        return testDataSets;
    }

    @Nullable
    private static Set<String> parseSetProperty(String name) {
        String prop = System.getProperty(name);
        return (prop == null || prop.isBlank()) ? null : Set.of(prop.split(", *"));
    }

    private static boolean isLookupDataset(TestDataset dataset) throws IOException {
        Settings settings = dataset.loadSettings();
        String mode = settings.get("index.mode");
        return (mode != null && mode.equalsIgnoreCase("lookup"));
    }

    private static boolean isSourceMappingDataset(TestDataset dataset) throws IOException {
        if (dataset.mappingFileName() == null) {
            return true;
        }
        JsonNode mappingNode = new ObjectMapper().readTree(dataset.streamMapping());
        // BWC tests don't support _source field mappings, so don't load those datasets.
        return mappingNode.get("_source") != null;
    }

    private static boolean isTimeSeries(TestDataset dataset) throws IOException {
        Settings settings = dataset.loadSettings();
        String mode = settings.get("index.mode");
        return (mode != null && mode.equalsIgnoreCase("time_series"));
    }

    public static void loadDataSetIntoEs(
        RestClient client,
        boolean supportsIndexModeLookup,
        boolean supportsSourceFieldMapping,
        boolean inferenceEnabled
    ) throws IOException {
        loadDataSetIntoEs(client, supportsIndexModeLookup, supportsSourceFieldMapping, inferenceEnabled, false, cap -> false, null);
    }

    private static final IndexCreator INDEX_CREATOR = (restClient, indexName, indexMapping, indexSettings) -> ESRestTestCase.createIndex(
        restClient,
        indexName,
        indexSettings,
        indexMapping,
        null,
        DEPRECATED_DEFAULT_METRIC_WARNING_HANDLER
    );

    public static void loadDataSetIntoEs(
        RestClient client,
        boolean supportsIndexModeLookup,
        boolean supportsSourceFieldMapping,
        boolean inferenceEnabled,
        boolean timeSeriesOnly,
        Predicate<EsqlCapabilities.Cap> capabilityCheck
    ) throws IOException {
        loadDataSetIntoEs(
            client,
            supportsIndexModeLookup,
            supportsSourceFieldMapping,
            inferenceEnabled,
            timeSeriesOnly,
            capabilityCheck,
            null
        );
    }

    /**
     * Load test datasets into Elasticsearch.
     *
     * @param indicesToLoad null to load all indices (default); empty list to load nothing; non-empty list to load only those indices.
     *                      When non-null, enrich policies whose source index is in this list are also loaded (unless skipped by
     *                      {@code tests.spec_indices} / {@code tests.spec_enrich_policies}).
     */
    public static void loadDataSetIntoEs(
        RestClient client,
        boolean supportsIndexModeLookup,
        boolean supportsSourceFieldMapping,
        boolean inferenceEnabled,
        boolean timeSeriesOnly,
        Predicate<EsqlCapabilities.Cap> capabilityCheck,
        @Nullable List<String> indicesToLoad
    ) throws IOException {
        if (indicesToLoad != null && indicesToLoad.isEmpty()) {
            return;
        }
        if (indicesToLoad != null) {
            // Restrict the requested indices to those actually supported on this cluster (inference, lookup mode,
            // source mapping, time-series, capabilities), mirroring the "load all" path. E.g. semantic_text is
            // skipped when the cluster has no inference test service; its tests skip too (required_capability).
            Set<String> available = new HashSet<>();
            for (TestDataset dataset : availableDatasetsForEs(
                supportsIndexModeLookup,
                supportsSourceFieldMapping,
                inferenceEnabled,
                timeSeriesOnly,
                capabilityCheck
            )) {
                available.add(dataset.indexName());
            }
            List<String> supportedIndices = new ArrayList<>();
            for (String indexName : indicesToLoad) {
                if (available.contains(indexName)) {
                    supportedIndices.add(indexName);
                }
            }
            loadDatasetsIntoEs(client, supportedIndices);
            if (timeSeriesOnly == false) {
                loadEnrichPoliciesForLoadedSourceIndices(client, supportedIndices);
            }
        } else {
            loadDataSets(
                client,
                supportsIndexModeLookup,
                supportsSourceFieldMapping,
                inferenceEnabled,
                timeSeriesOnly,
                capabilityCheck,
                INDEX_CREATOR
            );
            if (timeSeriesOnly == false) {
                loadEnrichPolicies(client);
            }
        }
        loadAliasesIntoEs(client, indicesToLoad);
    }

    /**
     * Loads enrich policies whose source index is in {@code loadedIndexNames}, mirroring
     * {@link #loadEnrichPolicies(RestClient)} rules for {@code tests.spec_indices} /
     * {@code tests.spec_enrich_policies}.
     */
    private static void loadEnrichPoliciesForLoadedSourceIndices(RestClient client, List<String> loadedIndexNames) throws IOException {
        if (specEnrichPolicies != null || specIndices == null) {
            Set<String> loaded = new HashSet<>(loadedIndexNames);
            logger.info("Loading enrich policies for loaded indices {}", loadedIndexNames);
            for (var policy : ENRICH_POLICIES.values()) {
                if (loaded.contains(policy.index()) == false) {
                    continue;
                }
                if (specEnrichPolicies != null && specEnrichPolicies.contains(policy.policyName()) == false) {
                    continue;
                }
                loadEnrichPolicy(client, policy);
            }
        }
    }

    /**
     * Load only the specified indices from CSV_DATASET into the cluster.
     * Used by external source tests that need lookup indices (e.g. languages_lookup) for LOOKUP JOIN.
     */
    public static void loadDatasetsIntoEs(RestClient client, List<String> indexNames) throws IOException {
        Set<String> loadedDatasets = new HashSet<>();
        for (String indexName : indexNames) {
            TestDataset dataset = CSV_DATASET.get(indexName);
            if (dataset == null) {
                throw new IllegalArgumentException("Dataset [" + indexName + "] not found in CSV_DATASET");
            }
            load(client, dataset, INDEX_CREATOR);
            loadedDatasets.add(dataset.indexName());
        }
        if (loadedDatasets.isEmpty() == false) {
            forceMerge(client, loadedDatasets);
        }
    }

    private static void loadDataSets(
        RestClient client,
        boolean supportsIndexModeLookup,
        boolean supportsSourceFieldMapping,
        boolean inferenceEnabled,
        boolean timeSeriesOnly,
        Predicate<EsqlCapabilities.Cap> capabilityCheck,
        IndexCreator indexCreator
    ) throws IOException {
        Set<String> loadedDatasets = new HashSet<>();
        logger.info("Loading test datasets");
        for (var dataset : availableDatasetsForEs(
            supportsIndexModeLookup,
            supportsSourceFieldMapping,
            inferenceEnabled,
            timeSeriesOnly,
            capabilityCheck
        )) {
            load(client, dataset, indexCreator);
            loadedDatasets.add(dataset.indexName);
        }
        forceMerge(client, loadedDatasets);
    }

    private static void loadEnrichPolicies(RestClient client) throws IOException {
        // Does not load any enrich policies if specIndices is set and specEnrichPolicies is not.
        if (specEnrichPolicies != null || specIndices == null) {
            logger.info("Loading enrich policies");
            for (var policy : ENRICH_POLICIES.values()) {
                if (specEnrichPolicies == null || specEnrichPolicies.contains(policy.policyName)) {
                    loadEnrichPolicy(client, policy);
                }
            }
        }
    }

    public static void loadViewsIntoEs(RestClient client) throws IOException {
        loadViewsIntoEs(client, cap -> true);
    }

    public static void loadViewsIntoEs(RestClient client, Predicate<EsqlCapabilities.Cap> capabilityCheck) throws IOException {
        loadViewsIntoEs(client, capabilityCheck, VIEW_CONFIGS.keySet());
    }

    /**
     * Loads exactly the named views (skipping any whose required capabilities are not enabled). Used by the
     * per-category test runners, which load only the views their category declares.
     */
    public static void loadViewsIntoEs(RestClient client, Predicate<EsqlCapabilities.Cap> capabilityCheck, Collection<String> viewNames)
        throws IOException {
        if (clusterSupportsViews(client)) {
            logger.info("Loading views {}", viewNames);
            for (String name : viewNames) {
                ViewConfig view = VIEW_CONFIGS.get(name);
                if (view == null) {
                    throw new IllegalArgumentException("View [" + name + "] not found in " + MANIFEST_RESOURCE);
                }
                if (view.requiredCapabilities.stream().allMatch(capabilityCheck) == false) {
                    logger.info("Skipping view [{}], missing required capabilities {}", view.name, view.requiredCapabilities);
                    continue;
                }
                loadView(client, view);
            }
        } else {
            logger.info("Skipping loading views as the cluster does not support views");
        }
    }

    private static void loadAliasesIntoEs(RestClient client) throws IOException {
        loadAliasesIntoEs(client, null);
    }

    /**
     * Creates index aliases from {@link #ALIAS_CONFIGS}. When {@code indicesToLoad} is non-null,
     * only aliases whose backing index is in that list are created — aliases for indices that were
     * not loaded in this run are skipped to avoid {@code index_not_found_exception}.
     */
    private static void loadAliasesIntoEs(RestClient client, @Nullable List<String> indicesToLoad) throws IOException {
        logger.info("Loading aliases");
        for (var alias : ALIAS_CONFIGS.values()) {
            if (indicesToLoad != null && indicesToLoad.contains(alias.indexName()) == false) {
                logger.debug("Skipping alias [{}] -> [{}]: backing index not in indicesToLoad", alias.aliasName(), alias.indexName());
                continue;
            }
            Request request = new Request("POST", "/_aliases");
            request.setJsonEntity(
                "{\"actions\":[{\"add\":{\"index\":\"" + alias.indexName() + "\",\"alias\":\"" + alias.aliasName() + "\"}}]}"
            );
            try {
                client.performRequest(request);
            } catch (ResponseException e) {
                // Alias may already exist (idempotent re-load); ignore 400
                if (e.getResponse().getStatusLine().getStatusCode() != 400) {
                    throw e;
                }
            }
        }
    }

    public static void deleteViews(RestClient client) throws IOException {
        deleteViews(client, VIEW_CONFIGS.keySet());
    }

    /** Deletes exactly the named views (used by the per-category delta on a category switch). No-op if absent. */
    public static void deleteViews(RestClient client, Collection<String> viewNames) throws IOException {
        if (clusterSupportsViews(client)) {
            logger.debug("Deleting views {}", viewNames);
            for (String name : viewNames) {
                deleteView(client, name);
            }
        } else {
            logger.info("Skipping deleting views as the cluster does not support views");
        }
    }

    /**
     * Moves the cluster's loaded index and enrich-policy data from the {@code currentIndices} set to the
     * {@code targetIndices} set by applying only the delta: deletes the indices/policies no longer needed, creates the
     * ones newly needed, and leaves shared ones untouched. Index data is fixed per index name, so a kept index is
     * already correct. This avoids the wipe-and-reload cost of {@link #deleteAllData} while leaving the loaded set
     * exactly equal to {@code targetIndices}, so a bare {@code FROM *} stays scoped to the loaded set.
     *
     * <p>The sets are the <em>requested</em> index names (e.g. a category's indices, or a suite's fixed override), not
     * pre-filtered; availability filtering happens here, mirroring {@link #loadDataSetIntoEs}: datasets unsupported on
     * this cluster (e.g. {@code semantic_text} without an inference service) are neither created nor counted, and
     * deleting an absent index is a no-op. Enrich policies are taken from the caller's declared lists
     * ({@code currentEnrich}/{@code targetEnrich}) rather than auto-derived from source indices, and are skipped
     * entirely on time-series-only clusters. Views are handled separately by the caller because they load through the
     * admin client and honour a capability check. Pass empty collections when nothing is loaded yet (every target index
     * and policy is then a create).
     */
    public static void syncIndicesAndEnrich(
        RestClient client,
        boolean supportsIndexModeLookup,
        boolean supportsSourceFieldMapping,
        boolean inferenceEnabled,
        boolean timeSeriesOnly,
        Predicate<EsqlCapabilities.Cap> capabilityCheck,
        Collection<String> currentIndices,
        Collection<String> targetIndices,
        Collection<String> currentEnrich,
        Collection<String> targetEnrich
    ) throws IOException {
        Set<String> available = new HashSet<>();
        for (TestDataset dataset : availableDatasetsForEs(
            supportsIndexModeLookup,
            supportsSourceFieldMapping,
            inferenceEnabled,
            timeSeriesOnly,
            capabilityCheck
        )) {
            available.add(dataset.indexName());
        }

        Set<String> current = availableSubset(currentIndices, available);
        Set<String> target = availableSubset(targetIndices, available);
        Set<String> currentEnrichSet = timeSeriesOnly ? Set.of() : applyEnrichFilter(currentEnrich);
        Set<String> targetEnrichSet = timeSeriesOnly ? Set.of() : applyEnrichFilter(targetEnrich);

        // Delete what is no longer needed: enrich policies first (they reference source indices), then indices.
        for (String policy : currentEnrichSet) {
            if (targetEnrichSet.contains(policy) == false) {
                deleteEnrichPolicy(client, policy);
            }
        }
        List<String> indicesToDelete = new ArrayList<>();
        for (String index : current) {
            if (target.contains(index) == false) {
                indicesToDelete.add(index);
            }
        }
        for (String index : indicesToDelete) {
            deleteIndex(client, index);
        }

        // Create what is newly needed: indices first (enrich executes against them), then enrich policies.
        List<String> indicesToCreate = new ArrayList<>();
        for (String index : target) {
            if (current.contains(index) == false) {
                indicesToCreate.add(index);
            }
        }
        loadDatasetsIntoEs(client, indicesToCreate);
        for (String policy : targetEnrichSet) {
            if (currentEnrichSet.contains(policy) == false) {
                loadEnrichPolicy(client, ENRICH_POLICIES.get(policy));
            }
        }
    }

    private static Set<String> availableSubset(Collection<String> names, Set<String> available) {
        Set<String> out = new HashSet<>();
        for (String name : names) {
            if (available.contains(name)) {
                out.add(name);
            }
        }
        return out;
    }

    /**
     * Returns the subset of {@code declared} enrich policies that pass the {@code tests.spec_enrich_policies} filter
     * (if set). When no filter is active, all declared policies are returned unchanged.
     */
    private static Set<String> applyEnrichFilter(Collection<String> declared) {
        if (specEnrichPolicies == null) {
            return new HashSet<>(declared);
        }
        Set<String> result = new HashSet<>();
        for (String policy : declared) {
            if (specEnrichPolicies.contains(policy)) {
                result.add(policy);
            }
        }
        return result;
    }

    /**
     * Wipes ALL test data (views, enrich policies, indices) from the cluster so the next category can be loaded into a
     * clean slate. Deleting an absent resource is ignored, so this is safe regardless of which subset was actually
     * loaded.
     *
     * <p>A full wipe — rather than deleting only the leaving category's declared indices — is required because
     * categories share indices, and because a load pulls in dependencies that are not listed in a category's own index
     * set (enrich source indices, and the indices queried by view definitions). A subset delete leaves those behind, so
     * the next category's load collides with {@code resource_already_exists_exception}. This mirrors CsvIT's unload-all
     * behaviour on a category switch and makes teardown independent of which category was previously loaded (including
     * across the per-variant test classes that share one cluster within a single JVM).
     */
    public static void deleteAllData(RestClient client) throws IOException {
        deleteViews(client);
        deleteEnrichPolicies(client);
        for (TestDataset dataset : CSV_DATASET.values()) {
            deleteIndex(client, dataset.indexName());
        }
    }

    private static void deleteIndexes(
        RestClient client,
        boolean supportsIndexModeLookup,
        boolean supportsSourceFieldMapping,
        boolean inferenceEnabled,
        boolean timeSeriesOnly,
        Predicate<EsqlCapabilities.Cap> capabilityCheck
    ) throws IOException {
        logger.info("Deleting test datasets");
        for (var dataset : availableDatasetsForEs(
            supportsIndexModeLookup,
            supportsSourceFieldMapping,
            inferenceEnabled,
            timeSeriesOnly,
            capabilityCheck
        )) {
            deleteIndex(client, dataset.indexName());
        }
    }

    private static void deleteEnrichPolicies(RestClient client) throws IOException {
        logger.debug("Deleting enrich policies");
        for (var policy : ENRICH_POLICIES.values()) {
            deleteEnrichPolicy(client, policy.policyName);
        }
    }

    public static void createInferenceEndpoints(RestClient client) throws IOException {
        createInferenceEndpoints(client, INFERENCE_CONFIGS.keySet());
    }

    /**
     * Creates only the listed inference endpoints (by id in {@link #INFERENCE_CONFIGS}), skipping any that already exist.
     */
    public static void createInferenceEndpoints(RestClient client, Collection<String> inferenceIds) throws IOException {
        for (String id : inferenceIds) {
            InferenceConfig config = INFERENCE_CONFIGS.get(id);
            if (config != null && hasInferenceEndpoint(client, config) == false) {
                createInferenceEndpoint(client, config);
            }
        }
    }

    public static void deleteInferenceEndpoints(RestClient client) throws IOException {
        for (var config : INFERENCE_CONFIGS.values()) {
            deleteInferenceEndpoint(client, config.id);
        }
    }

    public static void createInferenceEndpoint(RestClient client, InferenceConfig config) throws IOException {
        Request request = new Request("PUT", "/_inference/" + config.type.name() + "/" + config.id);
        request.setJsonEntity(config.loadConfig());
        client.performRequest(request);
    }

    private static boolean hasInferenceEndpoint(RestClient client, InferenceConfig config) throws IOException {
        Request request = new Request("GET", "/_inference/" + config.type.name() + "/" + config.id);
        try {
            client.performRequest(request);
        } catch (ResponseException e) {
            if (e.getResponse().getStatusLine().getStatusCode() == 404) {
                return false;
            }
            throw e;
        }
        return true;
    }

    public static void deleteInferenceEndpoint(RestClient client, String inferenceId) throws IOException {
        try {
            client.performRequest(new Request("DELETE", "/_inference/" + inferenceId));
        } catch (ResponseException e) {
            // 404 here means the endpoint was not created
            if (e.getResponse().getStatusLine().getStatusCode() != 404) {
                throw e;
            }
        }
    }

    public static void loadEnrichPolicy(RestClient client, EnrichConfig policy) throws IOException {
        logger.debug("Loading enrich policy [{}]", policy.policyName);
        Request request = new Request("PUT", "/_enrich/policy/" + policy.policyName);
        request.setJsonEntity(policy.loadPolicy());
        client.performRequest(request);

        request = new Request("POST", "/_enrich/policy/" + policy.policyName + "/_execute");
        client.performRequest(request);
    }

    private static void loadView(RestClient client, ViewConfig view) throws IOException {
        logger.debug("Loading view [{}] from file [/views/{}.esql]", view.name, view.name);
        Request request = new Request("PUT", "/_query/view/" + view.name);
        request.setJsonEntity("{\"query\":\"" + view.loadQuery().replace("\"", "\\\"").replace("\r", "").replace("\n", "") + "\"}");
        client.performRequest(request);
    }

    public static boolean clusterSupportsViews(RestClient client) throws IOException {
        // Step 1: check whether ALL nodes have basic views support (allMatch semantics).
        if (checkCapability(client, "POST", "/_query", "views_crud_as_index_actions") == false) {
            return false;
        }

        // Step 2: check whether ALL nodes support PUT /_query/view in the current cluster mode.
        // RestPutViewAction declares VIEWS_PUT_SERVERLESS_SCOPE in supportedCapabilities() only when
        // @ServerlessScope(Scope.PUBLIC) is present. Old serverless nodes lack this annotation and
        // therefore do not report this capability. /_capabilities with allMatch semantics returns
        // supported=false for any mixed cluster that contains such a node, correctly preventing view
        // loading from being attempted when it would fail on some nodes.
        //
        // In stateful mixed-cluster BWC tests where the old node has views but predates the
        // views_put_serverless_scope capability (introduced 2026-06-19), this check also returns
        // false and views tests are skipped rather than run. That is a conservative but safe
        // outcome: tests skip instead of failing with "index not found".
        return checkCapability(client, "PUT", "/_query/view/test", RestPutViewAction.VIEWS_PUT_SERVERLESS_SCOPE);
    }

    private static boolean checkCapability(RestClient client, String method, String path, String capability) throws IOException {
        Request capRequest = new Request("GET", "/_capabilities");
        capRequest.addParameter("method", method);
        capRequest.addParameter("path", path);
        capRequest.addParameter("capabilities", capability);
        try {
            Response capResponse = client.performRequest(capRequest);
            ObjectMapper mapper = new ObjectMapper();
            JsonNode json = mapper.readTree(capResponse.getEntity().getContent());
            JsonNode supported = json.get("supported");
            return supported != null && supported.asBoolean();
        } catch (ResponseException e) {
            return false;
        }
    }

    private static void deleteView(RestClient client, String viewName) throws IOException {
        final Set<Integer> ignoredDeleteStatusCodes = Set.of(400, 404, 405, 410, 500, 503);
        try {
            client.performRequest(new Request("DELETE", "/_query/view/" + viewName));
        } catch (ResponseException e) {
            // On older servers the view listing succeeds when it should not, so we get here when we should not, hence the 400 and 500.
            // 503 (master_not_discovered_exception) is transient and can occur in BWC mixed-cluster tests after node restarts.
            if (ignoredDeleteStatusCodes.contains(e.getResponse().getStatusLine().getStatusCode()) == false) {
                logger.info("View delete error: {}", e.getMessage());
                throw e;
            }
        }
    }

    private static void deleteIndex(RestClient client, String indexName) throws IOException {
        try {
            client.performRequest(new Request("DELETE", "/" + indexName));
        } catch (ResponseException e) {
            logger.info("Index delete error: {}", e.getMessage());
        }
    }

    private static void deleteEnrichPolicy(RestClient client, String policyName) throws IOException {
        try {
            client.performRequest(new Request("DELETE", "/_enrich/policy/" + policyName));
        } catch (ResponseException e) {
            logger.info("Enrich policy delete error: {}", e.getMessage());
        }
    }

    public static InputStream getResourceStream(String name) {
        return Objects.requireNonNull(CsvTestsDataLoader.class.getResourceAsStream(name), "Cannot find resource " + name);
    }

    public static String getResourceString(String name) {
        try (var stream = getResourceStream(name)) {
            return new String(stream.readAllBytes(), StandardCharsets.UTF_8);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    private static void load(RestClient client, TestDataset dataset, IndexCreator indexCreator) throws IOException {
        logger.debug("Loading dataset [{}] into ES index [{}]", dataset.dataFileName, dataset.indexName);
        indexCreator.createIndex(client, dataset.indexName, readMappingFile(dataset), dataset.loadSettings());

        // Some examples only test that the query and mappings are valid, and don't need example data. Use .noData() for those
        if (dataset.dataFileName != null) {
            loadCsvData(client, dataset.indexName, dataset.streamData(), dataset.allowSubFields);
        }
    }

    public static String readMappingFile(TestDataset dataset) throws IOException {
        String mappingJsonText = dataset.loadMappings();
        boolean hasTypeMappingOverrides = dataset.typeMapping != null && dataset.typeMapping.isEmpty() == false;
        if (hasTypeMappingOverrides == false && dataset.dynamic == null) {
            return mappingJsonText;
        }
        boolean modified = false;
        ObjectMapper mapper = new ObjectMapper();
        JsonNode mappingNode = mapper.readTree(mappingJsonText);

        if (hasTypeMappingOverrides) {
            for (Map.Entry<String, String> entry : dataset.typeMapping.entrySet()) {
                String key = entry.getKey();
                String newType = entry.getValue();

                // Navigate dotted paths to find the parent properties node and leaf field name.
                String[] segments = key.split("\\.");
                ObjectNode propertiesNode = (ObjectNode) mappingNode.path("properties");
                for (int i = 0; i < segments.length - 1 && propertiesNode != null; i++) {
                    JsonNode child = propertiesNode.get(segments[i]);
                    propertiesNode = child != null ? (ObjectNode) child.path("properties") : null;
                }
                String leafName = segments[segments.length - 1];

                if (propertiesNode == null) {
                    continue;
                }
                if (newType == null) {
                    // null value means remove the field from the mapping
                    if (propertiesNode.has(leafName)) {
                        propertiesNode.remove(leafName);
                        modified = true;
                    }
                } else if (propertiesNode.has(leafName)) {
                    ((ObjectNode) propertiesNode.get(leafName)).put("type", newType);
                    modified = true;
                }
            }
        }

        if (dataset.dynamic != null) {
            ((ObjectNode) mappingNode).put("dynamic", dataset.dynamic);
            modified = true;
        }

        if (modified) {
            return mapper.writerWithDefaultPrettyPrinter().writeValueAsString(mappingNode);
        }
        return mappingJsonText;
    }

    record Column(String name, String type) {}

    public record Document(String id, String slice, StringBuilder json) {}

    public static List<Document> readCsvDocuments(InputStream resource, boolean allowSubFields) {
        try (BufferedReader reader = reader(resource)) {
            var documents = new ArrayList<Document>();
            String line;
            int lineNumber = 1;
            Column[] columns = null; // Column info. If one column name contains dot, it is a subfield and its value will be null
            List<Integer> subFieldsIndices = new ArrayList<>(); // list containing the index of a subfield in "columns" String[]
            while ((line = reader.readLine()) != null) {
                line = line.trim();
                // ignore comments
                if (line.isEmpty() || line.startsWith("//")) {
                    continue;
                }
                String[] entries = multiValuesAwareCsvToStringArray(line, lineNumber);
                // the schema row
                if (columns == null) {
                    columns = parseHeaders(entries, allowSubFields, subFieldsIndices);
                }
                // data rows
                else {
                    if (entries.length != columns.length) {
                        throw new IllegalArgumentException(
                            format(
                                null,
                                "Error line [{}]: Incorrect number of entries; expected [{}] but found [{}]",
                                lineNumber,
                                columns.length,
                                entries.length
                            )
                        );
                    }
                    documents.add(parseDocument(columns, entries, lineNumber, subFieldsIndices));
                }
                lineNumber++;
            }
            return documents;
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    /**
     * Loads a classic csv file in an ES cluster using a RestClient.
     * The structure of the file is as follows:
     * - commented lines should start with "//"
     * - the first non-comment line from the file is the schema line (comma separated field_name:ES_data_type elements)
     *   - sub-fields should be placed after the root field using a dot notation for the name:
     *       root_field:long,root_field.sub_field:integer
     *   - a special _id field can be used in the schema and the values of this field will be used in the bulk request as actual doc ids
     * - all subsequent non-comment lines represent the values that will be used to build the _bulk request
     * - an empty string "" refers to a null value
     * - a value starting with an opening square bracket "[" and ending with a closing square bracket "]" refers to a multi-value field
     *   - multi-values are comma separated
     *   - commas inside multivalue fields can be escaped with \ (backslash) character
     */
    public static void loadCsvData(RestClient client, String indexName, InputStream resource, boolean allowSubFields) throws IOException {
        ArrayList<String> failures = new ArrayList<>();
        StringBuilder builder = new StringBuilder();
        try (BufferedReader reader = reader(resource)) {
            String line;
            int lineNumber = 1;
            Column[] columns = null; // Column info. If one column name contains dot, it is a subfield and its value will be null
            List<Integer> subFieldsIndices = new ArrayList<>(); // list containing the index of a subfield in "columns" String[]

            while ((line = reader.readLine()) != null) {
                line = line.trim();
                // ignore comments
                if (line.isEmpty() || line.startsWith("//")) {
                    continue;
                }
                String[] entries = multiValuesAwareCsvToStringArray(line, lineNumber);
                // the schema row
                if (columns == null) {
                    columns = parseHeaders(entries, allowSubFields, subFieldsIndices);
                }
                // data rows
                else {
                    if (entries.length != columns.length) {
                        throw new IllegalArgumentException(
                            format(
                                null,
                                "Error line [{}]: Incorrect number of entries; expected [{}] but found [{}]",
                                lineNumber,
                                columns.length,
                                entries.length
                            )
                        );
                    }
                    // id, document
                    var document = parseDocument(columns, entries, lineNumber, subFieldsIndices);

                    builder.append(
                        "{\"index\": {\"_index\":\""
                            + indexName
                            + "\""
                            + (document.id() != null ? ", \"_id\": \"" + document.id() + "\"" : "")
                            + (document.slice() != null ? ", \"_slice\": \"" + document.slice() + "\"" : "")
                            + "}}\n"
                    );
                    builder.append(document.json());
                }
                lineNumber++;
                if (builder.length() > BULK_DATA_SIZE) {
                    sendBulkRequest(indexName, builder, client, failures);
                    builder.setLength(0);
                }
            }
        }
        if (builder.isEmpty() == false) {
            sendBulkRequest(indexName, builder, client, failures);
        }
        if (failures.isEmpty() == false) {
            for (String failure : failures) {
                logger.error(failure);
            }
            throw new IOException("Data loading failed with " + failures.size() + " errors: " + failures.get(0));
        }
    }

    private static Column[] parseHeaders(String[] entries, boolean allowSubFields, List<Integer> subFieldsIndices) {
        var columns = new Column[entries.length];
        for (int i = 0; i < entries.length; i++) {
            int split = entries[i].indexOf(':');
            if (split < 0) {
                columns[i] = new Column(entries[i].trim(), null);
            } else {
                String name = entries[i].substring(0, split).trim();
                String type = entries[i].substring(split + 1).trim();
                if (allowSubFields || name.contains(".") == false) {
                    columns[i] = new Column(name, type);
                } else {// if it's a subfield, ignore it in the _bulk request
                    columns[i] = null;
                    subFieldsIndices.add(i);
                }
            }
        }
        return columns;
    }

    private static Document parseDocument(Column[] columns, String[] entries, int lineNumber, List<Integer> subFieldsIndices) {
        StringBuilder row = new StringBuilder("{");
        String id = null;
        String slice = null;
        for (int i = 0; i < entries.length; i++) {
            // ignore values that belong to subfields and don't add them to the bulk request
            if (subFieldsIndices.contains(i) == false) {
                if ("".equals(entries[i])) {
                    // Value is null, skip
                    continue;
                }
                if (columns[i] != null && "_id".equals(columns[i].name)) {
                    // Value is an _id
                    id = entries[i];
                    continue;
                }
                if (columns[i] != null && SliceIndexing.PARAM_NAME.equals(columns[i].name)) {
                    slice = entries[i];
                    continue;
                }

                try {
                    // add a comma after the previous value, only when there was actually a value before
                    if (i > 0 && row.length() > 1) {
                        row.append(",");
                    }
                    // split on comma ignoring escaped commas
                    String[] multiValues = entries[i].split(COMMA_ESCAPING_REGEX);
                    if (multiValues.length > 1) {
                        StringBuilder rowStringValue = new StringBuilder("[");
                        for (String s : multiValues) {
                            rowStringValue.append(toJson(columns[i].type, s)).append(",");
                        }
                        // remove the last comma and put a closing bracket instead
                        rowStringValue.replace(rowStringValue.length() - 1, rowStringValue.length(), "]");
                        entries[i] = rowStringValue.toString();
                    } else {
                        entries[i] = toJson(columns[i].type, entries[i]);
                    }
                    // replace any escaped commas with single comma
                    entries[i] = entries[i].replace(ESCAPED_COMMA_SEQUENCE, ",");
                    row.append("\"").append(columns[i].name).append("\":").append(entries[i]);
                } catch (Exception e) {
                    throw new IllegalArgumentException(
                        format(null, "Error line [{}]: Cannot parse entry [{}] with value [{}]", lineNumber, i + 1, entries[i]),
                        e
                    );
                }
            }
        }
        row.append("}\n");
        return new Document(id, slice, row);
    }

    private static final Pattern RANGE_PATTERN = Pattern.compile("([0-9\\-.Z:]+)\\.\\.([0-9\\-.Z:]+)");
    private static final String NUMERIC_REGEX = "-?\\d+(\\.\\d+)?";

    private static String toJson(String type, String value) {
        return switch (type == null ? "" : type) {
            case "date_range", "double_range", "integer_range" -> {
                Matcher m = RANGE_PATTERN.matcher(value);
                if (m.matches() == false) {
                    throw new IllegalArgumentException("can't parse range: " + value);
                }
                yield "{\"gte\": \"" + m.group(1) + "\", \"lt\": \"" + m.group(2) + "\"}";
            }
            // Text and keyword fields are always strings — strip outer quotes if present
            // (they are CSV formatting, not part of the value), escape inner quotes, and wrap.
            case "text", "keyword" -> {
                String content = value;
                if (content.startsWith("\"") && content.endsWith("\"")) {
                    content = content.substring(1, content.length() - 1);
                }
                yield "\"" + content.replace("\"", "\\\"") + "\"";
            }
            default -> {
                boolean isQuoted = (value.startsWith("\"") && value.endsWith("\"")) || (value.startsWith("{") && value.endsWith("}"));
                boolean isNumeric = value.matches(NUMERIC_REGEX);
                yield isQuoted || isNumeric ? value : "\"" + value + "\"";
            }
        };
    }

    private static void sendBulkRequest(String indexName, StringBuilder builder, RestClient client, List<String> failures)
        throws IOException {
        // The indexName is optional for a bulk request, but we use it for routing in MultiClusterSpecIT.
        builder.append("\n");
        logger.trace("Sending bulk request of [{}] bytes for [{}]", builder.length(), indexName);
        Request request = new Request("POST", "/" + indexName + "/_bulk");
        request.setJsonEntity(builder.toString());
        request.addParameter("refresh", "false"); // will be _forcemerge'd next
        Response response = client.performRequest(request);
        if (response.getStatusLine().getStatusCode() == 200) {
            HttpEntity entity = response.getEntity();
            try (InputStream content = entity.getContent()) {
                XContentType xContentType = XContentType.fromMediaType(entity.getContentType().getValue());
                Map<String, Object> result = XContentHelper.convertToMap(xContentType.xContent(), content, false);
                Object errors = result.get("errors");
                if (Boolean.TRUE.equals(errors)) {
                    addError(failures, indexName, builder, "errors: " + result);
                }
            }
        } else {
            addError(failures, indexName, builder, "status: " + response.getStatusLine());
        }
    }

    private static void addError(List<String> failures, String indexName, StringBuilder builder, String message) {
        failures.add(
            format(
                "Data loading of [{}] bytes into [{}] failed with {}: Data [{}...]",
                builder.length(),
                indexName,
                message,
                builder.substring(0, 100)
            )
        );
    }

    private static void forceMerge(RestClient client, Set<String> indices) throws IOException {
        String pattern = String.join(",", indices);

        Request request = new Request("POST", "/" + pattern + "/_forcemerge?max_num_segments=1");
        Response response = client.performRequest(request);
        if (response.getStatusLine().getStatusCode() != 200) {
            logger.warn("Force-merging [{}] to 1 segment failed: {}", pattern, response.getStatusLine());
        } else {
            logger.info("[{}] forced-merged to 1 segment", pattern);
        }
    }

    public record MultiIndexTestDataset(String indexPattern, List<TestDataset> datasets) {
        public static MultiIndexTestDataset of(TestDataset testsDataset) {
            return new MultiIndexTestDataset(testsDataset.indexName, List.of(testsDataset));
        }

    }

    public record TestDataset(
        String indexName,
        String mappingFileName,
        String dataFileName,
        String settingFileName,
        boolean allowSubFields,
        @Nullable Map<String, String> typeMapping,
        @Nullable Map<String, String> dynamicTypeMapping,
        @Nullable String dynamic,
        List<String> inferenceEndpoints,
        List<EsqlCapabilities.Cap> requiredCapabilities
    ) {

        public TestDataset(String indexName) {
            this(indexName, "mapping-" + indexName + ".json", indexName + ".csv", null, true, null, null, null, List.of(), List.of());
        }

        public TestDataset(String indexName, String mappingFileName, String dataFileName) {
            this(indexName, mappingFileName, dataFileName, null, true, null, null, null, List.of(), List.of());
        }

        public TestDataset(String indexName, String mappingFileName, String dataFileName, String settingFileName) {
            this(indexName, mappingFileName, dataFileName, settingFileName, true, null, null, null, List.of(), List.of());
        }

        public TestDataset withIndex(String indexName) {
            return new TestDataset(
                indexName,
                mappingFileName,
                dataFileName,
                settingFileName,
                allowSubFields,
                typeMapping,
                dynamicTypeMapping,
                dynamic,
                inferenceEndpoints,
                requiredCapabilities
            );
        }

        public TestDataset withData(String dataFileName) {
            return new TestDataset(
                indexName,
                mappingFileName,
                dataFileName,
                settingFileName,
                allowSubFields,
                typeMapping,
                dynamicTypeMapping,
                dynamic,
                inferenceEndpoints,
                requiredCapabilities
            );
        }

        public TestDataset noData() {
            return new TestDataset(
                indexName,
                mappingFileName,
                null,
                settingFileName,
                allowSubFields,
                typeMapping,
                dynamicTypeMapping,
                dynamic,
                inferenceEndpoints,
                requiredCapabilities
            );
        }

        public TestDataset withSetting(String settingFileName) {
            return new TestDataset(
                indexName,
                mappingFileName,
                dataFileName,
                settingFileName,
                allowSubFields,
                typeMapping,
                dynamicTypeMapping,
                dynamic,
                inferenceEndpoints,
                requiredCapabilities
            );
        }

        public TestDataset noSubfields() {
            return new TestDataset(
                indexName,
                mappingFileName,
                dataFileName,
                settingFileName,
                false,
                typeMapping,
                dynamicTypeMapping,
                dynamic,
                inferenceEndpoints,
                requiredCapabilities
            );
        }

        /**
         * Overrides the types of fields in the mapping file. Each entry maps a field name to its new type
         * (e.g. {@code Map.of("client_ip", "keyword")} changes client_ip from ip to keyword).
         * A {@code null} value removes the field from the mapping entirely, making it unmapped for this index.
         * <p>
         * This affects both the Elasticsearch index mapping (via {@link CsvTestsDataLoader#readMappingFile}) and
         * the in-memory mapping used by CSV unit tests.
         */
        public TestDataset withTypeMapping(Map<String, String> typeMapping) {
            return new TestDataset(
                indexName,
                mappingFileName,
                dataFileName,
                settingFileName,
                allowSubFields,
                typeMapping,
                dynamicTypeMapping,
                dynamic,
                inferenceEndpoints,
                requiredCapabilities
            );
        }

        /**
         * Adds field mappings that are not present in the mapping file, but will be in the field caps response, e.g. because during
         * ingestion more fields are added dynamically. Required for csv tests which do not ingest the csvs into real indices.
         */
        public TestDataset withDynamicTypeMapping(Map<String, String> dynamicTypeMapping) {
            if (dynamicTypeMapping != null && mappingFileName != null) {
                Map<String, EsField> mappedFields = LoadMapping.loadMapping(streamMapping());
                for (String fieldName : dynamicTypeMapping.keySet()) {
                    if (isMappedField(mappedFields, fieldName)) {
                        throw new IllegalArgumentException(
                            "Field ["
                                + fieldName
                                + "] in dynamicTypeMapping for dataset ["
                                + indexName
                                + "] is already mapped; use withTypeMapping instead"
                        );
                    }
                }
            }
            return new TestDataset(
                indexName,
                mappingFileName,
                dataFileName,
                settingFileName,
                allowSubFields,
                typeMapping,
                dynamicTypeMapping,
                dynamic,
                inferenceEndpoints,
                requiredCapabilities
            );
        }

        private static boolean isMappedField(Map<String, EsField> mapping, String fieldName) {
            String[] segments = fieldName.split("\\.");
            Map<String, EsField> currentMap = mapping;
            for (int i = 0; i < segments.length; i++) {
                EsField field = currentMap.get(segments[i]);
                if (field == null) {
                    return false;
                }
                if (i == segments.length - 1) {
                    return true;
                }
                currentMap = field.getProperties();
                if (currentMap == null || currentMap.isEmpty()) {
                    return false;
                }
            }
            return false;
        }

        /**
         * Sets the "dynamic" mapping parameter (e.g. "false", "strict", "runtime").
         * This prevents unmapped fields in the data from being automatically indexed.
         */
        public TestDataset withDynamic(String dynamic) {
            return new TestDataset(
                indexName,
                mappingFileName,
                dataFileName,
                settingFileName,
                allowSubFields,
                typeMapping,
                dynamicTypeMapping,
                dynamic,
                inferenceEndpoints,
                requiredCapabilities
            );
        }

        public TestDataset withInferenceEndpoints(String... inferenceEndpoints) {
            return new TestDataset(
                indexName,
                mappingFileName,
                dataFileName,
                settingFileName,
                allowSubFields,
                typeMapping,
                dynamicTypeMapping,
                dynamic,
                List.of(inferenceEndpoints),
                requiredCapabilities
            );
        }

        public TestDataset withRequiredCapabilities(EsqlCapabilities.Cap... requiredCapabilities) {
            return new TestDataset(
                indexName,
                mappingFileName,
                dataFileName,
                settingFileName,
                allowSubFields,
                typeMapping,
                dynamicTypeMapping,
                dynamic,
                inferenceEndpoints,
                List.of(requiredCapabilities)
            );
        }

        public Settings loadSettings() throws IOException {
            if (settingFileName == null) {
                return Settings.EMPTY;
            }
            final String settingName = "/index/settings/" + settingFileName;
            return Settings.builder().loadFromStream(settingName, getResourceStream(settingName), false).build();
        }

        public String loadMappings() {
            return getResourceString("/index/mappings/" + mappingFileName);
        }

        public InputStream streamMapping() {
            return getResourceStream("/index/mappings/" + mappingFileName);
        }

        public InputStream streamData() {
            return getResourceStream("/data/" + dataFileName);
        }
    }

    /**
     * A group of csv-spec files that share the same data needs: an explicit set of indices to load, the enrich
     * policies to load, and an explicit (possibly empty) set of views to load.
     * <p>
     * There is deliberately no "all indices" mode. A bare {@code FROM *} in a csv-spec file is category-scoped —
     * it matches exactly the indices this category loads — and a prefix wildcard like {@code employees*} resolves
     * the same as in production because {@link #indices} already includes every index that matches it. Views are
     * loaded only for the files that reference a view by name (the {@code views} category); this is what lets some
     * {@code FROM *} tests see views while others do not.
     */
    public record Category(String name, List<String> indices, List<String> enrich, List<String> views) {
        public boolean loadsViews() {
            return views.isEmpty() == false;
        }
    }

    public record EnrichConfig(String policyName, String policyFileName, String index) {
        public String loadPolicy() {
            return getResourceString("/enrich/policy/" + policyFileName);
        }

        public InputStream streamPolicy() {
            return getResourceStream("/enrich/policy/" + policyFileName);
        }
    }

    public record InferenceConfig(String id, TaskType type) {
        public String loadConfig() {
            return getResourceString("/inference/" + id + ".json");
        }
    }

    public record ViewConfig(String name, List<EsqlCapabilities.Cap> requiredCapabilities) {
        public ViewConfig(String name) {
            this(name, List.of());
        }

        public String loadQuery() {
            return getResourceString("/views/" + name + ".esql");
        }
    }

    /** An index alias to create alongside the main test indices. */
    public record AliasConfig(String aliasName, String indexName) {}

    private interface IndexCreator {
        void createIndex(RestClient client, String indexName, String mapping, Settings indexSettings) throws IOException;
    }
}
