/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.logsdb.qa.ecs;

import org.elasticsearch.xcontent.XContentBuilder;

import java.io.IOException;
import java.time.Instant;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.util.List;
import java.util.Locale;

/**
 * Generates the shared ECS-shaped mapping and synthetic log documents for
 * {@link EcsLogsEsqlDuelRestIT}. All methods are static; the class is not instantiable.
 *
 * <p>The mapping is intentionally narrow — a strict subset of ECS that both
 * {@code logsdb} and {@code logsdb_columnar} handle identically. See the javadoc
 * on {@link #writeMapping(XContentBuilder)} for field-by-field inclusion/exclusion rationale.
 */
public final class EcsLogsDataGenerator {

    /** Fixed start instant for the timestamp ladder so runs are reproducible. */
    static final Instant START = Instant.parse("2024-01-01T00:00:00Z");

    private static final DateTimeFormatter TS_FMT = DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ss.SSS'Z'", Locale.ROOT)
        .withZone(ZoneOffset.UTC);

    // ── value pools (bounded cardinality; must not grow with corpus size) ────────────────────

    static final String[] HOST_NAMES = {
        "alpha",
        "bravo",
        "charlie",
        "delta",
        "echo",
        "foxtrot",
        "golf",
        "hotel",
        "india",
        "juliet",
        "kilo",
        "lima",
        "mike",
        "november",
        "oscar",
        "papa",
        "quebec",
        "romeo",
        "sierra",
        "tango",
        "uniform",
        "victor",
        "whiskey",
        "xray",
        "yankee",
        "zulu",
        "node-a",
        "node-b",
        "node-c",
        "node-d",
        "node-e",
        "node-f",
        "node-g",
        "node-h",
        "node-i",
        "node-j",
        "worker-1",
        "worker-2",
        "worker-3",
        "worker-4",
        "worker-5",
        "worker-6",
        "db-primary",
        "db-replica",
        "cache-1",
        "cache-2",
        "lb-1",
        "lb-2",
        "gateway-1",
        "gateway-2" };

    static final String[] SERVICE_NAMES = {
        "frontend",
        "backend",
        "api-gateway",
        "auth-service",
        "user-service",
        "order-service",
        "payment-service",
        "inventory-service",
        "notification-service",
        "search-service",
        "recommendation-engine",
        "reporting-service",
        "analytics-service",
        "data-pipeline",
        "config-service",
        "session-service",
        "cache-service",
        "db-proxy",
        "scheduler",
        "worker" };

    static final String[] SERVICE_ENVS = { "production", "staging", "development", "testing", "qa" };

    static final String[] CLOUD_PROVIDERS = { "aws", "gcp", "azure", "alibaba" };
    static final String[] CLOUD_REGIONS = {
        "us-east-1",
        "us-west-2",
        "eu-west-1",
        "eu-central-1",
        "ap-southeast-1",
        "ap-northeast-1",
        "us-central1",
        "europe-west1" };
    static final String[] CLOUD_AZS = {
        "us-east-1a",
        "us-east-1b",
        "us-east-1c",
        "us-west-2a",
        "us-west-2b",
        "eu-west-1a",
        "eu-west-1b",
        "ap-southeast-1a",
        "ap-northeast-1a" };

    static final String[] LOG_LEVELS = { "TRACE", "DEBUG", "INFO", "WARN", "ERROR" };

    static final String[] HTTP_METHODS = { "GET", "POST", "PUT", "DELETE", "PATCH" };
    static final int[] HTTP_STATUS_CODES = { 200, 201, 204, 400, 401, 403, 404, 422, 429, 500, 503 };

    static final String[] URL_DOMAINS = {
        "api.example.com",
        "app.example.com",
        "static.example.com",
        "cdn.example.com",
        "auth.example.com" };
    static final String[] URL_PATHS = {
        "/api/v1/users",
        "/api/v1/orders",
        "/api/v1/products",
        "/api/v2/search",
        "/api/v2/recommendations",
        "/health",
        "/metrics",
        "/status",
        "/api/v1/auth/login",
        "/api/v1/auth/logout" };

    static final String[] EVENT_OUTCOMES = { "success", "failure", "unknown" };
    static final String[] EVENT_ACTIONS = {
        "login",
        "logout",
        "create",
        "read",
        "update",
        "delete",
        "search",
        "export",
        "import",
        "payment" };

    static final String[] CLIENT_IPS = {
        "203.0.113.1",
        "203.0.113.2",
        "198.51.100.1",
        "198.51.100.2",
        "192.0.2.1",
        "192.0.2.2",
        "10.10.0.1",
        "10.10.0.2" };
    static final String[] HOST_IPS = {
        "10.0.0.1",
        "10.0.0.2",
        "10.0.1.1",
        "10.0.1.2",
        "192.168.1.1",
        "192.168.1.2",
        "172.16.0.1",
        "172.16.0.2" };

    static final String[] ERROR_TYPES = {
        "java.lang.NullPointerException",
        "java.lang.IllegalArgumentException",
        "java.io.IOException",
        "java.util.concurrent.TimeoutException",
        "com.example.ServiceUnavailableException" };
    static final String[] ERROR_CODES = { "ERR_001", "ERR_002", "ERR_003", "ERR_004", "ERR_005" };
    static final String[] ERROR_MESSAGES = {
        "Connection refused",
        "Null pointer encountered",
        "Invalid argument provided",
        "Operation timed out",
        "Service unavailable",
        "Unexpected state" };

    static final String[] MESSAGES = {
        "Request processed successfully",
        "User authenticated",
        "Connection established",
        "Cache miss fetching from database",
        "Scheduled task completed",
        "Configuration reloaded",
        "Health check passed",
        "Metrics collected",
        "Session created",
        "Token refreshed" };

    /**
     * Pre-sorted tag arrays. Must be sorted so that columnar (which returns keyword arrays in
     * original array order) and logsdb (which returns them sorted/deduped from doc values)
     * produce identical results. See {@code KeywordFieldMapper} {@code readInArrayOrder}.
     */
    static final String[][] TAG_POOLS = {
        {},
        { "audit" },
        { "critical" },
        { "deprecated", "legacy" },
        { "experimental" },
        { "beta", "feature-flag" },
        { "canary", "monitoring", "production" },
        { "high-priority", "production" } };

    /**
     * Pre-sorted, unique URL arrays for the multi-valued {@code wildcard} field {@code url.full}.
     *
     * <p>{@code WildcardFieldMapper} sets {@code arrayOrderBinaryDocValues =
     * indexMode.isStrictColumnar()}, so {@code logsdb_columnar} returns wildcard arrays in original
     * insertion order while {@code logsdb} returns them sorted and deduped from doc values. A
     * pre-sorted unique array is identical under both encoders — the same invariant
     * {@link #TAG_POOLS} relies on, applied here to a wildcard mapper.
     *
     * <p><strong>Invariant:</strong> every inner array must be sorted in ascending lexicographic
     * order and contain no duplicate elements. Violating this makes the two index modes return
     * different sequences, causing a duel failure that is difficult to trace back to this pool.
     *
     * <p>A single-element entry must be emitted as a JSON scalar (not a one-element array) so
     * that synthetic {@code _source} also agrees: {@code ["a"]} round-trips as {@code "a"} in
     * columnar but preserves the array wrapper in logsdb. The duel is ES|QL-only today, but keeping
     * the invariant API-complete avoids silent breakage if {@code _source} comparison is added later.
     */
    static final String[][] URL_FULLS = {
        {},
        { "https://api.example.com/v1/status" },
        { "https://api.example.com/v1/users", "https://app.example.com/v2/search" },
        { "https://auth.example.com/login" },
        { "https://cdn.example.com/assets/main.js", "https://static.example.com/css/app.css" },
        { "https://api.example.com/v2/orders" },
        { "https://app.example.com/health", "https://app.example.com/metrics" },
        { "https://auth.example.com/logout" } };

    /**
     * User-agent strings for the {@code keyword} field {@code user_agent.original}.
     *
     * <p>Values are short, ASCII-only, and contain no {@code "} or {@code \} characters —
     * they are interpolated unescaped into both the bulk body and ES|QL string literals.
     * Parentheses and slashes are fine.
     */
    static final String[] USER_AGENTS = {
        "Mozilla/5.0 (X11; Linux x86_64) Chrome/120.0",
        "Mozilla/5.0 (Macintosh) Firefox/121.0",
        "curl/8.4.0",
        "Mozilla/5.0 (Windows NT 10.0) Chrome/120.0",
        "python-requests/2.31.0" };

    /**
     * Label sets for the {@code flattened} field {@code container.labels}. Each entry is an array
     * of key-value rows, where the first element of each row is the key and the remaining elements
     * are the values (pre-sorted ascending, duplicate-free).
     *
     * <p>{@code FlattenedFieldMapper.java:383-390} defaults {@code preserve_leaf_arrays} to
     * {@code LOSSY} in logsdb and {@code EXACT} in strict columnar. LOSSY drops exactly three
     * things: element order, duplicates, and JSON nulls. Every entry here is therefore a fixed
     * point of that transform, so both modes reconstruct the same JSON — the same technique
     * {@link #URL_FULLS} uses for wildcard.
     *
     * <p><strong>Invariants — all six are load-bearing:</strong>
     * <ol>
     *   <li>No JSON nulls anywhere.</li>
     *   <li>Leaf values are strings only. Columnar's EXACT path batch-parses values and would
     *       render a JSON number {@code 1.50} as {@code 1.5}; logsdb's LOSSY path would not.</li>
     *   <li>Each key appears exactly once, in a single root object — no dotted/nested duplicate
     *       spelling. The sorted-unique invariant holds per key <em>after</em> merging, so writing
     *       a key twice would reintroduce ordering.</li>
     *   <li>Multi-value rows are sorted ascending and duplicate-free.</li>
     *   <li>A single value is emitted as a JSON scalar, never as {@code ["a"]}.</li>
     *   <li>Keys and values are short ASCII, well under the default {@code ignore_above}.</li>
     * </ol>
     */
    static final String[][][] CONTAINER_LABEL_SETS = {
        { { "maintainer", "platform-team" }, { "tier", "backend" } },
        { { "env", "production" }, { "tier", "backend", "critical" } },
        { { "build", "stable" }, { "region", "us-east" }, { "team", "sre" } },
        { { "env", "staging" } },
        { { "rack", "rack-1", "rack-2" }, { "zone", "us-east-1a" } },
        { { "release", "v1" }, { "tier", "frontend" } },
        { { "datacenter", "dc-east" }, { "team", "infra" } },
        { { "tier", "backend", "database" }, { "zone", "eu-west-1a" } } };

    static final String[] LABEL_KEYS = { "env", "team", "region", "tier", "version", "build", "release", "datacenter", "rack", "zone" };
    static final String[] LABEL_VALUES = {
        "prod",
        "dev",
        "v1.2.3",
        "v2.0.0",
        "blue",
        "green",
        "primary",
        "secondary",
        "us-east",
        "eu-west" };

    static final String[] CONTAINER_NAMES;
    static {
        CONTAINER_NAMES = new String[200];
        for (int i = 0; i < CONTAINER_NAMES.length; i++) {
            CONTAINER_NAMES[i] = "container-" + String.format(Locale.ROOT, "%03d", i + 1);
        }
    }

    static final String[] HOST_ARCHS = { "x86_64", "aarch64", "arm64" };

    /**
     * All 30 distinct service version strings produced by {@link #appendDocument}.
     * The formula is {@code "v" + (1 + ordinal%3) + "." + (ordinal%10) + ".0"}, which maps to
     * index {@code (ordinal%3)*10 + (ordinal%10)}.
     */
    static final String[] SERVICE_VERSIONS;
    static {
        SERVICE_VERSIONS = new String[30];
        int idx = 0;
        for (int major = 1; major <= 3; major++) {
            for (int minor = 0; minor < 10; minor++) {
                SERVICE_VERSIONS[idx++] = "v" + major + "." + minor + ".0";
            }
        }
    }

    private EcsLogsDataGenerator() {}

    /**
     * Returns the ISO-8601 {@code @timestamp} value for the document at the given ordinal.
     * Used by {@link EcsEsqlQueryGenerator} to build date predicates anchored in the corpus window
     * rather than using a hardcoded range that may not overlap the actual data.
     */
    public static String timestampAt(int ordinal) {
        return TS_FMT.format(START.plusSeconds(ordinal));
    }

    // ── mapping ──────────────────────────────────────────────────────────────────────────────

    /**
     * Writes the shared ECS mapping root object into {@code b}. Byte-identical on both the
     * {@code logsdb} baseline and the {@code logsdb_columnar} contender.
     *
     * <p>All dotted property names (e.g. {@code "log.level"}) are accepted by both modes.
     * {@code logsdb} expands them to nested objects; {@code logsdb_columnar} auto-flattens the
     * nested form to the same dotted leaf names, so ES|QL field references resolve identically.
     *
     * <p>Field types included or excluded with rationale:
     * <ul>
     *   <li>{@code wildcard} – <strong>included</strong> as {@code url.path} (single-valued) and
     *       {@code url.full} (multi-valued). {@code WildcardFieldMapper.java:278} sets
     *       {@code arrayOrderBinaryDocValues = indexMode.isStrictColumnar()}, so columnar preserves
     *       original array order while logsdb sorts and dedupes. Both fields are constrained to
     *       shapes that read identically under either encoder: a single value is byte-identical in
     *       both modes, and {@link #URL_FULLS} arrays are pre-sorted and unique so logsdb's
     *       sort-and-dedupe is a no-op. <strong>Never emit {@code ["a"]}, {@code null}, or an
     *       unsorted/duplicate array for these fields.</strong> {@code error.stack_trace} stays
     *       dropped — single-valued but long, and not a priority for this suite.</li>
     *   <li>{@code keyword + match_only_text} multi-fields – <strong>included</strong> as
     *       {@code user_agent.original} (keyword) with a {@code .text} (match_only_text) sub-field.
     *       {@code doc_values} is pinned to {@code true} on the sub-field because
     *       {@code MatchOnlyTextFieldMapper.java:172-181} otherwise defaults it to
     *       {@code indexMode.isStrictColumnar()}, putting columnar on binary doc values and logsdb
     *       on {@code _source}. Pinning puts both modes on the same storage path. (The claim that
     *       sub-fields are aggregatable on one side but not the other is a {@code _field_caps}
     *       divergence; ES|QL hardcodes {@code aggregatable = false} for TEXT regardless, so it is
     *       invisible here.) The top-level {@code message} and {@code error.message} fields are left
     *       at their default {@code doc_values} — deliberately — so the suite also proves ES|QL
     *       agrees when the two modes read from different storage.</li>
     *   <li>{@code flattened} – <strong>included</strong> as {@code container.labels}. Array
     *       preservation defaults to {@code LOSSY} in logsdb and {@code EXACT} in strict columnar
     *       ({@code FlattenedFieldMapper.java:383-390}); LOSSY drops element order, duplicates, and
     *       JSON nulls. Values in {@link #CONTAINER_LABEL_SETS} satisfy all six constraints that
     *       make every entry a fixed point of the LOSSY transform, so both modes reconstruct
     *       identical JSON. <strong>Never add JSON nulls, unsorted/duplicate arrays, or numeric
     *       leaf values to that pool.</strong></li>
     *   <li>{@code ignore_above} – an over-long value goes to {@code _ignored_source} in logsdb
     *       but into binary doc values in columnar; invisible to ES|QL in one mode, visible in
     *       the other. Omitted; the generator keeps all keyword values short.</li>
     *   <li>{@code constant_keyword data_stream.*} – bulk is sent without the default pipeline
     *       so those fields are never populated.</li>
     *   <li>{@code geo_point} and {@code scaled_float} – not central to log querying; covered
     *       by {@code LogsDbSubobjectsFalseVersusLogsDbColumnarRestIT}.</li>
     * </ul>
     */
    public static void writeMapping(XContentBuilder b) throws IOException {
        b.startObject();
        b.field("date_detection", false);
        b.field("dynamic", true);

        // strings_as_keyword: our template outranks the built-in "logs" template and does not
        // inherit its dynamic templates. Without this, dynamic string fields become text+keyword
        // on both sides but then diverge on default doc_values for the text sub-field in columnar.
        b.startArray("dynamic_templates");
        b.startObject();
        b.startObject("strings_as_keyword");
        b.field("match_mapping_type", "string");
        b.startObject("mapping").field("type", "keyword").endObject();
        b.endObject();
        b.endObject();
        b.endArray();

        b.startObject("properties");

        // Core
        keyword(b, "@timestamp", "date");
        keyword(b, "log_id", "keyword");
        keyword(b, "message", "match_only_text");

        // log.*
        keyword(b, "log.level", "keyword");
        keyword(b, "log.logger", "keyword");
        keyword(b, "log.origin.function", "keyword");
        b.startObject("log.origin.file.line").field("type", "long").endObject();

        // host.*
        keyword(b, "host.name", "keyword");
        keyword(b, "host.architecture", "keyword");
        keyword(b, "host.os.name", "keyword");
        keyword(b, "host.ip", "ip");

        // service.*
        keyword(b, "service.name", "keyword");
        keyword(b, "service.version", "keyword");
        keyword(b, "service.environment", "keyword");
        keyword(b, "service.node.name", "keyword");

        // event.*
        keyword(b, "event.ingested", "date");
        keyword(b, "event.dataset", "keyword");
        keyword(b, "event.action", "keyword");
        keyword(b, "event.outcome", "keyword");
        b.startObject("event.duration").field("type", "long").endObject();
        // event.risk_score: the only double — exercises AVG/SUM floating-point epsilon handling
        b.startObject("event.risk_score").field("type", "double").endObject();

        // http.*
        keyword(b, "http.request.method", "keyword");
        b.startObject("http.request.bytes").field("type", "long").endObject();
        b.startObject("http.response.status_code").field("type", "long").endObject();
        b.startObject("http.response.bytes").field("type", "long").endObject();

        // url.* — url.path and url.full are wildcard (see inclusion note above); url.full is
        // multi-valued and emitted as a pre-sorted unique array so both index modes agree.
        keyword(b, "url.domain", "keyword");
        keyword(b, "url.path", "wildcard");
        keyword(b, "url.full", "wildcard");
        keyword(b, "url.query", "keyword");

        // user_agent.original: keyword + match_only_text sub-field (see inclusion note above)
        keywordWithTextSubfield(b, "user_agent.original");

        // user.*
        keyword(b, "user.name", "keyword");
        keyword(b, "user.id", "keyword");

        // network
        keyword(b, "client.ip", "ip");
        keyword(b, "source.ip", "ip");
        b.startObject("source.port").field("type", "long").endObject();
        b.startObject("network.bytes").field("type", "long").endObject();

        // error.* — error.stack_trace excluded (wildcard in ECS, not a priority for this suite)
        keyword(b, "error.type", "keyword");
        keyword(b, "error.code", "keyword");
        keyword(b, "error.message", "match_only_text");

        // tracing
        keyword(b, "trace.id", "keyword");
        keyword(b, "span.id", "keyword");
        keyword(b, "transaction.id", "keyword");

        // container / orchestrator
        keyword(b, "container.id", "keyword");
        keyword(b, "container.name", "keyword");
        // container.labels: flattened, not the same as the event-level labels object above;
        // values in CONTAINER_LABEL_SETS satisfy the LOSSY fixed-point invariant (see rationale).
        keyword(b, "container.labels", "flattened");
        keyword(b, "orchestrator.namespace", "keyword");
        keyword(b, "orchestrator.cluster.name", "keyword");

        // cloud.*
        keyword(b, "cloud.provider", "keyword");
        keyword(b, "cloud.region", "keyword");
        keyword(b, "cloud.availability_zone", "keyword");

        // process.*
        b.startObject("process.pid").field("type", "long").endObject();
        keyword(b, "process.name", "keyword");

        // tags: multi-valued keyword — values pre-sorted so both index modes return identical arrays
        keyword(b, "tags", "keyword");

        // labels: object with no explicit properties; sub-keys are dynamically mapped as keyword
        // via strings_as_keyword, exercising dynamic mapping and columnar auto-flattening
        b.startObject("labels").field("type", "object").field("dynamic", true).endObject();

        b.endObject(); // properties
        b.endObject(); // root
    }

    private static void keyword(XContentBuilder b, String name, String type) throws IOException {
        b.startObject(name).field("type", type).endObject();
    }

    /**
     * Writes a {@code keyword} property carrying a {@code match_only_text} sub-field named
     * {@code text} — the ECS multi-field shape.
     *
     * <p>{@code doc_values} is pinned to {@code true} on the sub-field because
     * {@code MatchOnlyTextFieldMapper.java:172-181} otherwise defaults it to
     * {@code indexMode.isStrictColumnar()}: columnar would read the sub-field from binary doc
     * values while logsdb reads it from {@code _source}. Pinning puts both modes on the same
     * storage path. The parameter is not updateable, so it must live in the static mapping.
     * {@code LogsDbSubobjectsFalseVersusLogsDbColumnarRestIT.java:236-247} pins it for the same
     * reason.
     */
    private static void keywordWithTextSubfield(XContentBuilder b, String name) throws IOException {
        b.startObject(name)
            .field("type", "keyword")
            .startObject("fields")
            .startObject("text")
            .field("type", "match_only_text")
            .field("doc_values", true)
            .endObject()
            .endObject()
            .endObject();
    }

    // ── document generation ───────────────────────────────────────────────────────────────────

    /**
     * Returns a newline-delimited {@code _bulk} request body for documents
     * {@code [firstOrdinal, firstOrdinal + count)}. Action lines use
     * {@code {"create":{"_id":"<ordinal>"}}} with no {@code _index}, so the same body can be
     * posted to both the baseline and contender data streams without modification.
     *
     * <p>All field values are derived deterministically from the ordinal so the body is
     * byte-identical regardless of how many times it is generated — which is the property that
     * makes "generate once, POST twice" correct.
     *
     * <p>Multi-valued fields ({@code tags}, {@code url.full}, dynamic {@code labels.*},
     * per-key arrays in {@code container.labels}) are pre-sorted and unique. Columnar returns
     * keyword and wildcard arrays in original insertion order ({@code KeywordFieldMapper
     * readInArrayOrder}, {@code MultiValuedBinaryDocValuesField.ArrayOrderInlineNull} for
     * wildcard), while logsdb returns them sorted and deduped from doc values. A pre-sorted unique
     * array is identical under both readers. The flattened field {@code container.labels} is
     * additionally constrained so that its values are a fixed point of the LOSSY transform (no
     * nulls, no duplicates, pre-sorted, string leaves only) — see {@link #CONTAINER_LABEL_SETS}.
     * Single-valued wildcard fields ({@code url.path}) are byte-identical in both modes because
     * both encoders special-case a single value and store the raw bytes directly.
     */
    public static String bulkBatch(int firstOrdinal, int count) {
        // Estimate ~400 bytes per document (action line + source)
        StringBuilder sb = new StringBuilder(count * 450);
        for (int i = 0; i < count; i++) {
            int ordinal = firstOrdinal + i;
            sb.append("{\"create\":{\"_id\":\"").append(String.format(Locale.ROOT, "%010d", ordinal)).append("\"}}\n");
            appendDocument(sb, ordinal);
            sb.append('\n');
        }
        return sb.toString();
    }

    @SuppressWarnings("checkstyle:MethodLength")
    private static void appendDocument(StringBuilder sb, int ordinal) {
        Instant ts = START.plusSeconds(ordinal);
        sb.append('{');

        // Always-present fields (every document has these)
        appendStr(sb, "@timestamp", TS_FMT.format(ts), true);
        appendStr(sb, "log_id", String.format(Locale.ROOT, "%010d", ordinal), false);
        appendStr(sb, "log.level", LOG_LEVELS[ordinal % LOG_LEVELS.length], false);
        appendStr(sb, "host.name", HOST_NAMES[ordinal % HOST_NAMES.length], false);
        appendStr(sb, "host.architecture", HOST_ARCHS[ordinal % HOST_ARCHS.length], false);
        appendStr(sb, "service.name", SERVICE_NAMES[ordinal % SERVICE_NAMES.length], false);
        appendStr(sb, "cloud.provider", CLOUD_PROVIDERS[ordinal % CLOUD_PROVIDERS.length], false);
        appendStr(sb, "cloud.region", CLOUD_REGIONS[ordinal % CLOUD_REGIONS.length], false);
        appendStr(sb, "container.name", CONTAINER_NAMES[ordinal % CONTAINER_NAMES.length], false);

        // Optional core fields — omit ~10% deterministically to exercise sparse doc values
        if (mask(ordinal, 1) != 0) {
            appendStr(sb, "message", MESSAGES[ordinal % MESSAGES.length], false);
        }
        if (mask(ordinal, 2) != 0) {
            appendStr(sb, "service.environment", SERVICE_ENVS[ordinal % SERVICE_ENVS.length], false);
        }
        // service.version: present ~80% of documents (slot 10); 30 distinct values from SERVICE_VERSIONS
        if ((ordinal * 7 + 10) % 10 < 8) {
            appendStr(sb, "service.version", SERVICE_VERSIONS[(ordinal % 3) * 10 + (ordinal % 10)], false);
        }
        if (mask(ordinal, 3) != 0) {
            appendStr(sb, "cloud.availability_zone", CLOUD_AZS[ordinal % CLOUD_AZS.length], false);
        }
        if (mask(ordinal, 4) != 0) {
            appendStr(sb, "host.ip", HOST_IPS[ordinal % HOST_IPS.length], false);
        }
        if (mask(ordinal, 5) != 0) {
            appendStr(sb, "log.logger", SERVICE_NAMES[ordinal % SERVICE_NAMES.length] + ".Main", false);
        }

        // event.ingested: ~50% of documents
        if ((ordinal * 7 + 6) % 2 == 0) {
            appendStr(sb, "event.ingested", TS_FMT.format(ts.plusSeconds(30)), false);
        }

        // event.risk_score: the only double (~60% of docs) — exercises AVG/SUM epsilon handling
        if ((ordinal * 7 + 7) % 10 < 6) {
            sb.append(",\"event.risk_score\":").append(String.format(java.util.Locale.ROOT, "%.1f", (ordinal % 1000) / 10.0));
        }

        // Flavor-specific fields (0=HTTP access, 1=app error, 2=plain app)
        int flavor = ordinal % 3;
        if (flavor == 0) {
            appendStr(sb, "http.request.method", HTTP_METHODS[ordinal % HTTP_METHODS.length], false);
            appendLong(sb, "http.response.status_code", HTTP_STATUS_CODES[ordinal % HTTP_STATUS_CODES.length], false);
            appendLong(sb, "http.request.bytes", (ordinal % 4096) + 64L, false);
            appendLong(sb, "http.response.bytes", (ordinal % 65536) + 128L, false);
            appendStr(sb, "url.domain", URL_DOMAINS[ordinal % URL_DOMAINS.length], false);
            appendStr(sb, "url.path", URL_PATHS[ordinal % URL_PATHS.length], false);
            appendWildcardMv(sb, "url.full", URL_FULLS[ordinal % URL_FULLS.length]);
            appendStr(sb, "user_agent.original", USER_AGENTS[ordinal % USER_AGENTS.length], false);
            appendStr(sb, "client.ip", CLIENT_IPS[ordinal % CLIENT_IPS.length], false);
            appendLong(sb, "event.duration", (ordinal % 10_000) * 1_000_000L, false);
            appendStr(sb, "event.outcome", HTTP_STATUS_CODES[ordinal % HTTP_STATUS_CODES.length] < 400 ? "success" : "failure", false);
        } else if (flavor == 1) {
            appendStr(sb, "error.type", ERROR_TYPES[ordinal % ERROR_TYPES.length], false);
            appendStr(sb, "error.code", ERROR_CODES[ordinal % ERROR_CODES.length], false);
            appendStr(sb, "error.message", ERROR_MESSAGES[ordinal % ERROR_MESSAGES.length], false);
            appendStr(sb, "trace.id", hexId(ordinal, 32), false);
            appendStr(sb, "span.id", hexId(ordinal, 16), false);
            appendStr(sb, "event.outcome", "failure", false);
        } else {
            appendLong(sb, "process.pid", (ordinal % 65535) + 1L, false);
            appendStr(sb, "process.name", SERVICE_NAMES[ordinal % SERVICE_NAMES.length] + "-proc", false);
            appendStr(sb, "orchestrator.namespace", "ns-" + (ordinal % 10), false);
            appendStr(sb, "event.action", EVENT_ACTIONS[ordinal % EVENT_ACTIONS.length], false);
            appendLong(sb, "event.duration", (ordinal % 5_000) * 1_000_000L, false);
            appendStr(sb, "event.outcome", EVENT_OUTCOMES[ordinal % EVENT_OUTCOMES.length], false);
        }

        // container.labels: flattened (~90% of docs); values are LOSSY fixed points (see pool javadoc)
        if (mask(ordinal, 9) != 0) {
            appendFlattened(sb, "container.labels", CONTAINER_LABEL_SETS[ordinal % CONTAINER_LABEL_SETS.length]);
        }

        // tags: pre-sorted multi-valued keyword (absent on ~12% of docs)
        String[] tags = TAG_POOLS[ordinal % TAG_POOLS.length];
        if (tags.length > 0) {
            sb.append(",\"tags\":[");
            for (int t = 0; t < tags.length; t++) {
                if (t > 0) sb.append(',');
                sb.append('"').append(tags[t]).append('"');
            }
            sb.append(']');
        }

        // Dynamic labels.* (~40% of docs, 1-2 keys) — exercises dynamic mapping + columnar
        // auto-flattening of the labels object. Key set is bounded (LABEL_KEYS.length) so
        // COUNT_DISTINCT on a labels.* field stays exact.
        if ((ordinal * 7 + 8) % 10 < 4) {
            String k1 = LABEL_KEYS[ordinal % LABEL_KEYS.length];
            String v1 = LABEL_VALUES[ordinal % LABEL_VALUES.length];
            sb.append(",\"labels\":{\"").append(k1).append("\":\"").append(v1).append('"');
            if ((ordinal * 7 + 9) % 10 < 3) {
                // Second label key: offset by pool length to avoid collision with k1
                int idx2 = (ordinal + LABEL_KEYS.length / 2) % LABEL_KEYS.length;
                String k2 = LABEL_KEYS[idx2];
                if (k2.equals(k1) == false) {
                    String v2 = LABEL_VALUES[(ordinal + 3) % LABEL_VALUES.length];
                    sb.append(",\"").append(k2).append("\":\"").append(v2).append('"');
                }
            }
            sb.append('}');
        }

        sb.append('}');
    }

    private static void appendStr(StringBuilder sb, String name, String value, boolean first) {
        if (first == false) sb.append(',');
        sb.append('"').append(name).append("\":\"").append(value).append('"');
    }

    /**
     * Emits a {@code wildcard}-mapped field that may have 0, 1, or multiple values.
     *
     * <ul>
     *   <li>0 values – field is omitted entirely (both modes: field absent).</li>
     *   <li>1 value – emitted as a JSON scalar string, NOT as a one-element array. A one-element
     *       array {@code ["a"]} diverges in synthetic {@code _source}: logsdb preserves the array
     *       wrapper via {@code _ignored_source} while columnar stores the single value and emits
     *       a scalar. This duel is ES|QL-only so the divergence would be invisible today, but
     *       keeping the invariant API-complete avoids silent breakage if {@code _source} comparison
     *       is added later.</li>
     *   <li>≥2 values – emitted as a JSON array. The caller is responsible for passing a
     *       pre-sorted unique array so that logsdb's sort-and-dedupe is a no-op and the two modes
     *       return identical sequences. See {@link #URL_FULLS}.</li>
     * </ul>
     */
    private static void appendWildcardMv(StringBuilder sb, String name, String[] values) {
        if (values.length == 0) {
            return;
        }
        sb.append(",\"").append(name).append("\":");
        if (values.length == 1) {
            sb.append('"').append(values[0]).append('"');
        } else {
            sb.append('[');
            for (int i = 0; i < values.length; i++) {
                if (i > 0) sb.append(',');
                sb.append('"').append(values[i]).append('"');
            }
            sb.append(']');
        }
    }

    /**
     * Emits a {@code flattened}-mapped field as a root JSON object.
     *
     * <p>Each row in {@code labelRows} is {@code [key, val1, val2, ...]}. A row with a single
     * value emits the value as a JSON scalar; a row with multiple values emits a JSON array. The
     * caller is responsible for passing rows that satisfy all six invariants in
     * {@link #CONTAINER_LABEL_SETS} — in particular, multi-value rows must be sorted ascending and
     * duplicate-free so that logsdb's LOSSY transform is a no-op.
     */
    private static void appendFlattened(StringBuilder sb, String name, String[][] labelRows) {
        sb.append(",\"").append(name).append("\":{");
        for (int i = 0; i < labelRows.length; i++) {
            if (i > 0) sb.append(',');
            String[] row = labelRows[i];
            sb.append('"').append(row[0]).append("\":");
            if (row.length == 2) {
                sb.append('"').append(row[1]).append('"');
            } else {
                sb.append('[');
                for (int v = 1; v < row.length; v++) {
                    if (v > 1) sb.append(',');
                    sb.append('"').append(row[v]).append('"');
                }
                sb.append(']');
            }
        }
        sb.append('}');
    }

    private static void appendLong(StringBuilder sb, String name, long value, boolean first) {
        if (first == false) sb.append(',');
        sb.append('"').append(name).append("\":").append(value);
    }

    /** Returns 0 ~10% of the time (deterministic from ordinal + slot), non-zero otherwise. */
    private static int mask(int ordinal, int slot) {
        return (ordinal * 7 + slot) % 10;
    }

    /** Deterministic hex string of exactly {@code length} characters derived from {@code seed}. */
    private static String hexId(int seed, int length) {
        StringBuilder hex = new StringBuilder(length + 8);
        int h = seed;
        while (hex.length() < length) {
            h = h * 1664525 + 1013904223; // Knuth LCG
            hex.append(String.format(Locale.ROOT, "%08x", h & 0xFFFFFFFFL));
        }
        return hex.substring(0, length);
    }

    // ── field catalog ─────────────────────────────────────────────────────────────────────────

    /**
     * Describes a field in the ECS mapping with the metadata needed by
     * {@link EcsEsqlQueryGenerator} to generate type-correct, safe queries.
     *
     * @param name            dotted ES|QL field name
     * @param esqlType        ES|QL type string (keyword, long, double, date, ip, text)
     * @param sortable        safe to use in ORDER BY (not text, not ip, not multi-valued)
     * @param lowCardinality  distinct-value count bounded well below 3000 — safe for
     *                        COUNT_DISTINCT and VALUES (multi-valued fields excluded from VALUES)
     * @param multiValued     may contain multiple values per document
     * @param alwaysPresent   {@code true} if {@link #appendDocument} emits this field on every
     *                        document; {@code false} if it is conditionally emitted. Only fields
     *                        where {@code alwaysPresent == false} should be used in {@code IS NULL}
     *                        predicates — otherwise the predicate is provably vacuous.
     */
    public record Field(
        String name,
        String esqlType,
        boolean sortable,
        boolean lowCardinality,
        boolean multiValued,
        boolean alwaysPresent
    ) {}

    /**
     * Returns the complete catalog of fields that {@link EcsEsqlQueryGenerator} may reference.
     * {@code match_only_text} fields ({@code message}, {@code error.message},
     * {@code user_agent.original.text}) appear as type {@code "text"} — usable in {@code MATCH}
     * and {@code KEEP} but not in {@code SORT} or {@code STATS BY}.
     *
     * <p>The sixth constructor argument is {@code alwaysPresent}: {@code true} means
     * {@link #appendDocument} emits this field on every document, so {@code IS NULL} on it
     * is always false. Only fields with {@code alwaysPresent == false} should be used in
     * {@code IS NULL} predicates.
     */
    public static List<Field> fields() {
        return List.of(
            // name type sort lowCard multi alwaysPresent
            new Field("@timestamp", "date", true, false, false, true),
            new Field("log_id", "keyword", true, false, false, true),
            new Field("message", "text", false, false, false, false),  // ~90% present (mask slot 1)
            new Field("log.level", "keyword", true, true, false, true),
            new Field("log.logger", "keyword", true, false, false, false), // ~90% present (mask slot 5)
            new Field("host.name", "keyword", true, true, false, true),
            new Field("host.architecture", "keyword", true, true, false, true),
            new Field("host.ip", "ip", false, true, false, false), // ~90% present (mask slot 4)
            new Field("service.name", "keyword", true, true, false, true),
            new Field("service.version", "keyword", true, true, false, false), // ~80% present (slot 10)
            new Field("service.environment", "keyword", true, true, false, false), // ~90% present (mask slot 2)
            new Field("event.ingested", "date", true, false, false, false), // ~50% present
            new Field("event.action", "keyword", true, true, false, false), // flavor 2 only (~33%)
            new Field("event.outcome", "keyword", true, true, false, true),  // all flavors
            new Field("event.duration", "long", true, false, false, false), // flavors 0+2 (~67%)
            new Field("event.risk_score", "double", true, false, false, false), // ~60% present
            new Field("http.request.method", "keyword", true, true, false, false), // flavor 0 only (~33%)
            new Field("http.request.bytes", "long", true, false, false, false), // flavor 0 only (~33%)
            new Field("http.response.status_code", "long", true, true, false, false), // flavor 0 only (~33%)
            new Field("http.response.bytes", "long", true, false, false, false), // flavor 0 only (~33%)
            new Field("url.domain", "keyword", true, true, false, false), // flavor 0 only (~33%)
            new Field("url.path", "keyword", true, false, false, false), // flavor 0 only; mapped wildcard, surfaces as keyword in ESQL
            new Field("url.full", "keyword", false, true, true, false), // flavor 0 only; wildcard MV, pre-sorted unique
            new Field("user_agent.original", "keyword", true, true, false, false), // flavor 0 only; keyword parent of match_only_text
                                                                                   // sub-field
            new Field("user_agent.original.text", "text", false, false, false, false), // flavor 0 only; match_only_text sub-field,
                                                                                       // doc_values pinned
            new Field("client.ip", "ip", false, true, false, false), // flavor 0 only (~33%)
            new Field("error.type", "keyword", true, true, false, false), // flavor 1 only (~33%)
            new Field("error.code", "keyword", true, true, false, false), // flavor 1 only (~33%)
            new Field("error.message", "text", false, false, false, false), // flavor 1 only (~33%)
            new Field("trace.id", "keyword", true, false, false, false), // flavor 1 only (~33%)
            new Field("container.name", "keyword", true, true, false, true),
            new Field("cloud.provider", "keyword", true, true, false, true),
            new Field("cloud.region", "keyword", true, true, false, true),
            new Field("cloud.availability_zone", "keyword", true, true, false, false), // ~90% present (mask slot 3)
            new Field("process.pid", "long", true, false, false, false), // flavor 2 only (~33%)
            new Field("process.name", "keyword", true, false, false, false), // flavor 2 only (~33%)
            // container.labels: flattened — not sortable (DataType.FLATTENED excluded from
            // isSortable); lowCardinality=false keeps it out of COUNT_DISTINCT/VALUES; multiValued=false
            // because the root is one JSON string per row. ~90% present (mask slot 9).
            new Field("container.labels", "flattened", false, false, false, false),
            // tags: multi-valued — excluded from SORT and STATS BY; used in WHERE and KEEP.
            // ~87.5% present (TAG_POOLS[0] is empty; 1/8 ordinals have no tags).
            new Field("tags", "keyword", false, true, true, false)
        );
    }
}
