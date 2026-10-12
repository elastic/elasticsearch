/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasources;

import org.apache.lucene.util.BytesRef;
import org.elasticsearch.common.lucene.BytesRefs;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.expression.MetadataAttribute;
import org.elasticsearch.xpack.esql.datasources.PartitionFilterHintExtractor.Operator;
import org.elasticsearch.xpack.esql.datasources.PartitionFilterHintExtractor.PartitionFilterHint;

import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.OffsetDateTime;
import java.time.YearMonth;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.time.format.DateTimeParseException;
import java.time.temporal.ChronoField;
import java.time.temporal.TemporalAccessor;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.function.Consumer;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Overlay that maps a file-column filter onto path keys. Layout
 * ({@code partition_detection} / {@code partition_path}) still finds the keys;
 * this string says how a payload clock projects onto a subset of them.
 *
 * <p>{@link #validate} is registration-only and strict. {@link #fromConfig} is
 * query-time and lenient so a later grammar tightening cannot break a stored
 * dataset. {@link #CONFIG_KEYS} always lists the key.
 */
public final class PartitionSpec {

    public static final String CONFIG_PARTITION_SPEC = "partition_spec";

    /** The keys {@link #fromConfig} reads. */
    public static final Set<String> CONFIG_KEYS = Set.of(CONFIG_PARTITION_SPEC);

    public static final PartitionSpec EMPTY = new PartitionSpec(List.of());

    private static final String LEGAL_TRANSFORMS = "identity, year, month, day, hour";
    private static final String LEGAL_UNITS = "epoch_second, epoch_millis";
    /**
     * ES|QL unquoted identifiers ({@code @timestamp} included) plus {@code -} so {@code aws-region} stays bare.
     * Anything else is a backtick-quoted name.
     */
    private static final String IDENTIFIER_BODY = "[A-Za-z0-9_-]";
    static final String IDENTIFIER_RULE = "[A-Za-z]" + IDENTIFIER_BODY + "*|[@_]" + IDENTIFIER_BODY + "+|`backtick-quoted`";
    private static final Pattern IDENTIFIER = Pattern.compile("[A-Za-z]" + IDENTIFIER_BODY + "*|[@_]" + IDENTIFIER_BODY + "+");
    private static final Pattern SURFACED_RESERVED = Pattern.compile(
        "_partition\\.(?:[A-Za-z]" + IDENTIFIER_BODY + "*|[@_]" + IDENTIFIER_BODY + "+)"
    );

    static final int WRONG_UNIT_YEAR_MIN = 1971;
    static final int WRONG_UNIT_YEAR_MAX = 2100;

    public enum Transform {
        IDENTITY,
        YEAR,
        MONTH,
        DAY,
        HOUR;

        static Transform parse(String token) {
            return switch (token.toLowerCase(Locale.ROOT)) {
                case "identity" -> IDENTITY;
                case "year" -> YEAR;
                case "month" -> MONTH;
                case "day" -> DAY;
                case "hour" -> HOUR;
                default -> null;
            };
        }

        String token() {
            return name().toLowerCase(Locale.ROOT);
        }

        boolean isTemporal() {
            return this != IDENTITY;
        }
    }

    public enum Unit {
        EPOCH_SECOND,
        EPOCH_MILLIS;

        static Unit parse(String token) {
            return switch (token.toLowerCase(Locale.ROOT)) {
                case "epoch_second" -> EPOCH_SECOND;
                case "epoch_millis" -> EPOCH_MILLIS;
                default -> null;
            };
        }

        String token() {
            return name().toLowerCase(Locale.ROOT);
        }
    }

    /**
     * One bind: path {@code key} is {@code transform(column[, unit])}.
     *
     * @param key       surfaced path key (Hive folder name or template placeholder)
     * @param transform closed transform set
     * @param column    file column the query filter names
     * @param unit      how a numeric source is read; unused for {@link Transform#IDENTITY}
     */
    public record Field(String key, Transform transform, String column, Unit unit) {
        public Field {
            Objects.requireNonNull(key, "key");
            Objects.requireNonNull(transform, "transform");
            Objects.requireNonNull(column, "column");
            Objects.requireNonNull(unit, "unit");
        }

        /** Canonical spelling used in error and warning text. */
        public String describe() {
            if (transform == Transform.IDENTITY) {
                return key.equals(column) ? key : key + "=" + column;
            }
            String call = transform.token() + "(" + column + (unit == Unit.EPOCH_MILLIS ? "" : ", " + unit.token()) + ")";
            return key.equals(transform.token()) ? call : key + "=" + call;
        }
    }

    /**
     * Half-open UTC millis range {@code [startInclusive, endExclusive)}. A null
     * end is unbounded on that side.
     */
    public record InstantRange(@Nullable Long startInclusiveMillis, @Nullable Long endExclusiveMillis) {
        static final InstantRange ALL = new InstantRange(null, null);
        static final InstantRange EMPTY = new InstantRange(0L, 0L);

        boolean isEmpty() {
            return startInclusiveMillis != null && endExclusiveMillis != null && startInclusiveMillis >= endExclusiveMillis;
        }

        boolean isBounded() {
            return startInclusiveMillis != null && endExclusiveMillis != null && isEmpty() == false;
        }

        InstantRange intersect(InstantRange other) {
            Long start = maxInclusive(startInclusiveMillis, other.startInclusiveMillis);
            Long end = minExclusive(endExclusiveMillis, other.endExclusiveMillis);
            return new InstantRange(start, end);
        }

        boolean overlapsFolder(long folderStart, long folderEnd) {
            if (isEmpty()) {
                return false;
            }
            long rangeStart = startInclusiveMillis != null ? startInclusiveMillis : Long.MIN_VALUE;
            long rangeEnd = endExclusiveMillis != null ? endExclusiveMillis : Long.MAX_VALUE;
            return folderStart < rangeEnd && rangeStart < folderEnd;
        }

        private static Long maxInclusive(@Nullable Long a, @Nullable Long b) {
            if (a == null) {
                return b;
            }
            if (b == null) {
                return a;
            }
            return Math.max(a, b);
        }

        private static Long minExclusive(@Nullable Long a, @Nullable Long b) {
            if (a == null) {
                return b;
            }
            if (b == null) {
                return a;
            }
            return Math.min(a, b);
        }
    }

    private final List<Field> fields;

    PartitionSpec(List<Field> fields) {
        this.fields = List.copyOf(fields);
    }

    public boolean isEmpty() {
        return fields.isEmpty();
    }

    public List<Field> fields() {
        return fields;
    }

    /**
     * Every spec source column: temporal binds and identity remaps. Listing and
     * overlap extract against this set, not hive keys.
     */
    public Set<String> boundColumns() {
        Set<String> columns = new LinkedHashSet<>();
        for (Field field : fields) {
            columns.add(field.column());
        }
        return columns;
    }

    /**
     * Query-time parse: missing or unreadable values become {@link #EMPTY}.
     * Never throws on a stored value.
     */
    public static PartitionSpec fromConfig(@Nullable Map<String, Object> config) {
        if (config == null || config.isEmpty()) {
            return EMPTY;
        }
        Object value = config.get(CONFIG_PARTITION_SPEC);
        if (value instanceof String spec) {
            try {
                return parse(spec);
            } catch (IllegalArgumentException e) {
                return EMPTY;
            }
        }
        return EMPTY;
    }

    /**
     * Query-time notice when the key is present but {@link #validate} would reject it.
     * {@link #fromConfig} still returns {@link #EMPTY} for an unreadable string so a stored
     * dataset does not fail the query. {@code null} when the spec is absent or valid.
     */
    @Nullable
    public static String unusableNotice(@Nullable Map<String, Object> config) {
        if (config == null || config.containsKey(CONFIG_PARTITION_SPEC) == false) {
            return null;
        }
        try {
            validate(config);
        } catch (IllegalArgumentException e) {
            return e.getMessage();
        }
        return null;
    }

    /**
     * Closed {@code @timestamp} bounds from the request filter, as listing hints, on paths whose
     * spec binds that column. Other paths keep {@code hints}. An open range is not represented here:
     * the extractor only returns bounds when both ends parsed.
     */
    public static Map<String, List<PartitionFilterHint>> addTimestampBounds(
        Map<String, List<PartitionFilterHint>> hints,
        @Nullable Map<String, Map<String, Object>> pathConfigs,
        @Nullable Instant start,
        @Nullable Instant end
    ) {
        if (start == null || end == null || start.isAfter(end) || pathConfigs == null || pathConfigs.isEmpty()) {
            return hints;
        }
        Map<String, List<PartitionFilterHint>> withBounds = null;
        for (Map.Entry<String, Map<String, Object>> entry : pathConfigs.entrySet()) {
            PartitionSpec spec = fromConfig(entry.getValue());
            if (spec.bindsColumn(MetadataAttribute.TIMESTAMP_FIELD) == false) {
                continue;
            }
            if (withBounds == null) {
                withBounds = new LinkedHashMap<>(hints);
            }
            List<PartitionFilterHint> existing = withBounds.getOrDefault(entry.getKey(), List.of());
            List<PartitionFilterHint> merged = new ArrayList<>(existing.size() + 2);
            merged.add(new PartitionFilterHint(MetadataAttribute.TIMESTAMP_FIELD, Operator.GREATER_THAN_OR_EQUAL, List.of(start)));
            merged.add(new PartitionFilterHint(MetadataAttribute.TIMESTAMP_FIELD, Operator.LESS_THAN_OR_EQUAL, List.of(end)));
            merged.addAll(existing);
            withBounds.put(entry.getKey(), List.copyOf(merged));
        }
        return withBounds == null ? hints : withBounds;
    }

    private boolean bindsColumn(String column) {
        for (Field field : fields) {
            if (field.column().equals(column)) {
                return true;
            }
        }
        return false;
    }

    /**
     * PUT-time check. A {@code partition_detection: none} contradiction is
     * reported even when the string is also unparseable.
     */
    public static void validate(@Nullable Map<String, Object> config) {
        if (config == null || config.isEmpty() || config.containsKey(CONFIG_PARTITION_SPEC) == false) {
            return;
        }

        PartitionConfig.Strategy declared = declaredDetection(config.get(PartitionConfig.CONFIG_PARTITIONING_DETECTION));
        if (declared == PartitionConfig.Strategy.NONE) {
            throw new IllegalArgumentException(
                "["
                    + CONFIG_PARTITION_SPEC
                    + "] is set but partition detection is disabled, so the spec would be ignored; remove ["
                    + CONFIG_PARTITION_SPEC
                    + "] or enable partition detection"
            );
        }

        Object raw = config.get(CONFIG_PARTITION_SPEC);
        if (raw instanceof String == false) {
            throw new IllegalArgumentException("[" + CONFIG_PARTITION_SPEC + "] [" + raw + "] must be a non-empty string");
        }
        String spec = (String) raw;
        if (spec.isBlank()) {
            throw new IllegalArgumentException("[" + CONFIG_PARTITION_SPEC + "] must be a non-empty string");
        }

        PartitionSpec parsed = parse(spec);

        Object templateValue = config.get(PartitionConfig.CONFIG_PARTITIONING_PATH);
        String template = templateValue != null ? templateValue.toString() : null;
        if (template != null && template.isEmpty() == false) {
            Set<String> placeholders = new LinkedHashSet<>();
            for (String name : TemplatePartitionDetector.parseTemplateColumns(template)) {
                placeholders.add(ReservedPartitionNames.surface(name));
            }
            for (Field field : parsed.fields) {
                if (placeholders.contains(field.key()) == false) {
                    throw new IllegalArgumentException(
                        "["
                            + CONFIG_PARTITION_SPEC
                            + "] key ["
                            + field.key()
                            + "] is not a placeholder in ["
                            + PartitionConfig.CONFIG_PARTITIONING_PATH
                            + "] ["
                            + template
                            + "]; every spec key must be a {name} segment of the path template, or omit ["
                            + PartitionConfig.CONFIG_PARTITIONING_PATH
                            + "] for a Hive layout"
                    );
                }
            }
        }
    }

    @Nullable
    private static PartitionConfig.Strategy declaredDetection(@Nullable Object detectionValue) {
        if (detectionValue == null) {
            return null;
        }
        try {
            return PartitionConfig.Strategy.parse(detectionValue.toString());
        } catch (IllegalArgumentException e) {
            return null;
        }
    }

    /** Strict grammar parse. Empty or blank input is rejected. */
    public static PartitionSpec parse(String spec) {
        if (spec == null || spec.isBlank()) {
            throw new IllegalArgumentException("[" + CONFIG_PARTITION_SPEC + "] must be a non-empty string");
        }
        List<String> parts = splitTopLevel(spec);
        if (parts.isEmpty()) {
            throw new IllegalArgumentException("[" + CONFIG_PARTITION_SPEC + "] must be a non-empty string");
        }
        List<Field> parsed = new ArrayList<>(parts.size());
        Set<String> keys = new LinkedHashSet<>();
        for (String part : parts) {
            String trimmed = part.trim();
            if (trimmed.isEmpty()) {
                throw new IllegalArgumentException("[" + CONFIG_PARTITION_SPEC + "] has an empty field; remove the extra comma");
            }
            Field field = parseField(trimmed);
            if (keys.add(field.key()) == false) {
                throw new IllegalArgumentException(
                    "[" + CONFIG_PARTITION_SPEC + "] names key [" + field.key() + "] more than once; each partition key must appear once"
                );
            }
            parsed.add(field);
        }
        rejectMixedUnits(parsed);
        return new PartitionSpec(parsed);
    }

    private static void rejectMixedUnits(List<Field> parsed) {
        Map<String, Unit> units = new LinkedHashMap<>();
        for (Field field : parsed) {
            if (field.transform().isTemporal() == false) {
                continue;
            }
            Unit previous = units.putIfAbsent(field.column(), field.unit());
            if (previous != null && previous != field.unit()) {
                throw new IllegalArgumentException(
                    "["
                        + CONFIG_PARTITION_SPEC
                        + "] names column ["
                        + field.column()
                        + "] with units ["
                        + previous.token()
                        + "] and ["
                        + field.unit().token()
                        + "]; use one unit per source column"
                );
            }
        }
    }

    private static List<String> splitTopLevel(String spec) {
        List<String> parts = new ArrayList<>();
        int depth = 0;
        int start = 0;
        for (int i = 0; i < spec.length(); i++) {
            char c = spec.charAt(i);
            if (c == '`') {
                i = skipQuoted(spec, i);
                continue;
            }
            if (c == '(') {
                depth++;
            } else if (c == ')') {
                depth--;
                if (depth < 0) {
                    throw new IllegalArgumentException(
                        "[" + CONFIG_PARTITION_SPEC + "] [" + spec.trim() + "] has a stray closing [)]; check the field list"
                    );
                }
            } else if (c == ',' && depth == 0) {
                parts.add(spec.substring(start, i));
                start = i + 1;
            }
        }
        if (depth != 0) {
            throw new IllegalArgumentException("[" + CONFIG_PARTITION_SPEC + "] [" + spec.trim() + "] is missing a closing [)]");
        }
        parts.add(spec.substring(start));
        return parts;
    }

    private static Field parseField(String field) {
        int eq = indexOfTopLevel(field, '=');
        if (eq >= 0) {
            String key = parseIdentifier(field, field.substring(0, eq).trim());
            String rhs = field.substring(eq + 1).trim();
            if (rhs.isEmpty()) {
                throw new IllegalArgumentException(
                    "["
                        + CONFIG_PARTITION_SPEC
                        + "] ["
                        + field
                        + "] is missing a column after [=]; write [key=column] or [key=transform(column)]"
                );
            }
            if (rhs.indexOf('(') >= 0) {
                return parseTransformCall(field, key, rhs);
            }
            return new Field(key, Transform.IDENTITY, parseIdentifier(field, rhs), Unit.EPOCH_MILLIS);
        }
        if (field.indexOf('(') >= 0) {
            return parseTransformCall(field, null, field);
        }
        String column = parseIdentifier(field, field);
        return new Field(column, Transform.IDENTITY, column, Unit.EPOCH_MILLIS);
    }

    private static Field parseTransformCall(String field, @Nullable String explicitKey, String call) {
        int open = call.indexOf('(');
        if (open <= 0) {
            throw new IllegalArgumentException("[" + CONFIG_PARTITION_SPEC + "] [" + field + "] is missing a transform name before [(]");
        }
        String transformToken = call.substring(0, open).trim();
        Transform transform = Transform.parse(transformToken);
        if (transform == null) {
            throw new IllegalArgumentException(
                "["
                    + CONFIG_PARTITION_SPEC
                    + "] ["
                    + field
                    + "] has an unknown transform ["
                    + transformToken
                    + "]; legal transforms are ["
                    + LEGAL_TRANSFORMS
                    + "]"
            );
        }
        int close = matchingClose(call, open);
        String leftover = call.substring(close + 1).trim();
        if (leftover.isEmpty() == false) {
            throw new IllegalArgumentException(
                "["
                    + CONFIG_PARTITION_SPEC
                    + "] ["
                    + field
                    + "] has leftover text ["
                    + leftover
                    + "] after the field; remove ["
                    + leftover
                    + "]"
            );
        }
        String inside = call.substring(open + 1, close);
        List<String> args = splitTopLevel(inside);
        List<String> trimmedArgs = new ArrayList<>(args.size());
        for (String arg : args) {
            String t = arg.trim();
            if (t.isEmpty() == false) {
                trimmedArgs.add(t);
            } else if (args.size() > 1) {
                throw new IllegalArgumentException(
                    "[" + CONFIG_PARTITION_SPEC + "] [" + field + "] has an empty argument; remove the extra comma"
                );
            }
        }
        if (trimmedArgs.isEmpty()) {
            throw new IllegalArgumentException(
                "[" + CONFIG_PARTITION_SPEC + "] [" + field + "] is missing a column; write [" + transform.token() + "(column)]"
            );
        }
        if (trimmedArgs.size() > 2) {
            throw new IllegalArgumentException(
                "["
                    + CONFIG_PARTITION_SPEC
                    + "] ["
                    + field
                    + "] has leftover text ["
                    + trimmedArgs.get(2)
                    + "] after the field; remove ["
                    + trimmedArgs.get(2)
                    + "]"
            );
        }
        String column = parseIdentifier(field, trimmedArgs.get(0));
        Unit unit = Unit.EPOCH_MILLIS;
        if (trimmedArgs.size() == 2) {
            if (transform == Transform.IDENTITY) {
                throw new IllegalArgumentException(
                    "["
                        + CONFIG_PARTITION_SPEC
                        + "] ["
                        + field
                        + "] does not take a unit; identity does not take a unit — remove ["
                        + trimmedArgs.get(1)
                        + "] or use a temporal transform"
                );
            }
            unit = Unit.parse(trimmedArgs.get(1));
            if (unit == null) {
                throw new IllegalArgumentException(
                    "["
                        + CONFIG_PARTITION_SPEC
                        + "] ["
                        + field
                        + "] has an unknown unit ["
                        + trimmedArgs.get(1)
                        + "]; temporal transforms take ["
                        + LEGAL_UNITS
                        + "] or omit the unit (default epoch_millis)"
                );
            }
        }
        String key = explicitKey != null ? explicitKey : transform.token();
        return new Field(key, transform, column, unit);
    }

    private static int matchingClose(String call, int open) {
        int depth = 0;
        for (int i = open; i < call.length(); i++) {
            char c = call.charAt(i);
            if (c == '`') {
                i = skipQuoted(call, i);
                continue;
            }
            if (c == '(') {
                depth++;
            } else if (c == ')') {
                depth--;
                if (depth == 0) {
                    return i;
                }
            }
        }
        throw new IllegalArgumentException("[" + CONFIG_PARTITION_SPEC + "] [" + call.trim() + "] is missing a closing [)]");
    }

    private static int indexOfTopLevel(String text, char needle) {
        int depth = 0;
        for (int i = 0; i < text.length(); i++) {
            char c = text.charAt(i);
            if (c == '`') {
                i = skipQuoted(text, i);
                continue;
            }
            if (c == '(') {
                depth++;
            } else if (c == ')') {
                depth--;
            } else if (c == needle && depth == 0) {
                return i;
            }
        }
        return -1;
    }

    private static String parseIdentifier(String field, String raw) {
        if (raw.isEmpty()) {
            throw new IllegalArgumentException(
                "[" + CONFIG_PARTITION_SPEC + "] [" + field + "] has an invalid identifier []; identifiers are [" + IDENTIFIER_RULE + "]"
            );
        }
        if (raw.charAt(0) == '`') {
            return parseQuotedIdentifier(field, raw);
        }
        Matcher reserved = SURFACED_RESERVED.matcher(raw);
        if (reserved.matches()) {
            return raw;
        }
        Matcher ident = IDENTIFIER.matcher(raw);
        if (ident.matches() == false) {
            throw new IllegalArgumentException(
                "["
                    + CONFIG_PARTITION_SPEC
                    + "] ["
                    + field
                    + "] has an invalid identifier ["
                    + raw
                    + "]; identifiers are ["
                    + IDENTIFIER_RULE
                    + "]"
            );
        }
        return raw;
    }

    private static String parseQuotedIdentifier(String field, String raw) {
        int close = skipQuoted(raw, 0);
        if (close != raw.length() - 1) {
            throw new IllegalArgumentException(
                "["
                    + CONFIG_PARTITION_SPEC
                    + "] ["
                    + field
                    + "] has leftover text ["
                    + raw.substring(close + 1)
                    + "] after the quoted identifier"
            );
        }
        String inner = raw.substring(1, close).replace("``", "`");
        if (inner.isEmpty()) {
            throw new IllegalArgumentException("[" + CONFIG_PARTITION_SPEC + "] [" + field + "] has an empty quoted identifier");
        }
        return inner;
    }

    /** Index of the closing backtick. {@code openTick} points at the opening one. {@code ``} is one escaped backtick. */
    private static int skipQuoted(String text, int openTick) {
        for (int i = openTick + 1; i < text.length(); i++) {
            if (text.charAt(i) != '`') {
                continue;
            }
            if (i + 1 < text.length() && text.charAt(i + 1) == '`') {
                i++;
                continue;
            }
            return i;
        }
        throw new IllegalArgumentException("[" + CONFIG_PARTITION_SPEC + "] [" + text.trim() + "] has an unclosed backtick quote");
    }

    /**
     * Identity remaps rewrite the hint column to the path key. Temporal binds
     * on a source column emit a finite {@code year IN (...)} for the coarsest
     * year-key only — never an independent month IN list. Source-column hints
     * ({@code @timestamp}, {@code ts}) are then dropped so they cannot join
     * listing-cache identity once the glob stays walkable ({@code year=2024/**}).
     * They stay when no year IN is emitted, so a numeric bound that lands
     * outside 1971–2100 still reaches {@link #emitListingNotices}.
     */
    public List<PartitionFilterHint> projectListingHints(List<PartitionFilterHint> hints) {
        return projectListingHints(hints, null);
    }

    public List<PartitionFilterHint> projectListingHints(List<PartitionFilterHint> hints, @Nullable Set<String> detectedKeys) {
        if (isEmpty() || hints == null || hints.isEmpty()) {
            return hints == null ? List.of() : hints;
        }
        List<PartitionFilterHint> projected = new ArrayList<>(hints.size() + 2);
        boolean emittedYearIn = false;
        for (PartitionFilterHint hint : hints) {
            List<Field> remaps = identityRemaps(hint.columnName(), detectedKeys);
            if (remaps.isEmpty()) {
                projected.add(hint);
            } else {
                for (Field remap : remaps) {
                    projected.add(new PartitionFilterHint(remap.key(), hint.operator(), hint.values()));
                }
            }
        }
        for (Map.Entry<String, List<Field>> group : temporalGroups().entrySet()) {
            List<Field> binds = usable(group.getValue(), detectedKeys);
            if (binds.isEmpty()) {
                continue;
            }
            Field yearBind = coarsestYearBind(binds);
            if (yearBind == null) {
                continue;
            }
            SourceBounds bounds = sourceBounds(hints, group.getKey(), yearBind.unit());
            List<Integer> years = overlappingYears(bounds);
            if (years.isEmpty()) {
                continue;
            }
            List<Object> values = new ArrayList<>(years.size());
            values.addAll(years);
            projected.add(new PartitionFilterHint(yearBind.key(), Operator.IN, values));
            emittedYearIn = true;
        }
        return List.copyOf(emittedYearIn ? dropTemporalSourceHints(projected) : projected);
    }

    /**
     * Listing keys are path columns ({@code year}, remapped {@code aws-region}). A
     * temporal source is not one, unless that source is itself the path key.
     */
    private List<PartitionFilterHint> dropTemporalSourceHints(List<PartitionFilterHint> projected) {
        Set<String> temporalSources = temporalGroups().keySet();
        Set<String> keys = new LinkedHashSet<>();
        for (Field field : fields) {
            keys.add(field.key());
        }
        List<PartitionFilterHint> kept = new ArrayList<>(projected.size());
        for (PartitionFilterHint hint : projected) {
            if (temporalSources.contains(hint.columnName()) && keys.contains(hint.columnName()) == false) {
                continue;
            }
            kept.add(hint);
        }
        return kept;
    }

    /**
     * Copies identity-remap path values under the query column name so
     * {@code WHERE region == "eu"} matches Hive {@code aws-region=eu} at split
     * time. Listing already remaps the hint; the split matcher still sees the
     * original filter expression.
     */
    public void aliasIdentityValues(Map<String, Object> partitionValues) {
        if (isEmpty() || partitionValues == null || partitionValues.isEmpty()) {
            return;
        }
        for (Field field : fields) {
            if (field.transform() != Transform.IDENTITY || field.key().equals(field.column())) {
                continue;
            }
            if (partitionValues.containsKey(field.key()) && partitionValues.containsKey(field.column()) == false) {
                partitionValues.put(field.column(), partitionValues.get(field.key()));
            }
        }
    }

    /**
     * Folder interval vs the source range implied by hints on a spec column.
     * No source-column hint → {@code true} (identity matcher already ran).
     */
    public boolean overlaps(Map<String, Object> partitionValues, List<PartitionFilterHint> hints) {
        return overlaps(partitionValues, hints, partitionValues == null ? null : partitionValues.keySet());
    }

    public boolean overlaps(Map<String, Object> partitionValues, List<PartitionFilterHint> hints, @Nullable Set<String> detectedKeys) {
        if (isEmpty() || hints == null || hints.isEmpty() || partitionValues == null || partitionValues.isEmpty()) {
            return true;
        }
        for (Map.Entry<String, List<Field>> group : temporalGroups().entrySet()) {
            if (hasHintOn(hints, group.getKey()) == false) {
                continue;
            }
            List<Field> binds = usable(group.getValue(), detectedKeys);
            if (binds.isEmpty()) {
                continue;
            }
            Field finest = finestBind(binds, partitionValues);
            if (finest == null) {
                continue;
            }
            long[] folder = folderInterval(binds, partitionValues, finest.transform());
            if (folder == null) {
                continue;
            }
            SourceBounds bounds = sourceBounds(hints, group.getKey(), finest.unit());
            if (bounds.overlaps(folder[0], folder[1]) == false) {
                return false;
            }
        }
        return true;
    }

    /**
     * Same overlap test against resolved ancestor filter expressions (split
     * discovery). Missing source column → no overlap test. DATETIME/DATE_NANOS
     * literals become {@link Instant} so they cannot be scaled as unix numbers.
     */
    public boolean overlapsExpressions(Map<String, Object> partitionValues, List<Expression> filters) {
        if (isEmpty() || filters == null || filters.isEmpty()) {
            return true;
        }
        return overlaps(
            partitionValues,
            PartitionFilterHintExtractor.fromConjuncts(filters, Set.of(), boundColumns(), PartitionFilterHintExtractor.TEMPORAL)
        );
    }

    /** Notices for unmatched keys and a numeric bound that lands outside 1971–2100. */
    public void emitListingNotices(@Nullable Set<String> detectedKeys, @Nullable List<PartitionFilterHint> hints, Consumer<String> sink) {
        if (isEmpty() || sink == null) {
            return;
        }
        if (detectedKeys != null) {
            for (Field field : fields) {
                if (detectedKeys.contains(field.key()) == false) {
                    sink.accept(
                        "["
                            + CONFIG_PARTITION_SPEC
                            + "] bind ["
                            + field.describe()
                            + "] names key ["
                            + field.key()
                            + "] which was not detected in the path; the bind is ignored"
                    );
                }
            }
            emitMissingCoarserGrain(detectedKeys, sink);
        }
        if (hints == null || hints.isEmpty()) {
            return;
        }
        for (Map.Entry<String, List<Field>> group : temporalGroups().entrySet()) {
            List<Field> binds = group.getValue();
            Field sample = binds.get(0);
            for (PartitionFilterHint hint : hints) {
                if (hint.columnName().equals(group.getKey()) == false) {
                    continue;
                }
                for (Object raw : hint.values()) {
                    if (isNumericLiteral(raw) == false) {
                        continue;
                    }
                    Long millis = toUtcMillis(raw, sample.unit());
                    if (millis == null) {
                        continue;
                    }
                    int year = utcYear(millis);
                    if (year < WRONG_UNIT_YEAR_MIN || year > WRONG_UNIT_YEAR_MAX) {
                        sink.accept(
                            "["
                                + CONFIG_PARTITION_SPEC
                                + "] bind ["
                                + sample.describe()
                                + "] projects a calendar year ["
                                + year
                                + "] outside "
                                + WRONG_UNIT_YEAR_MIN
                                + "–"
                                + WRONG_UNIT_YEAR_MAX
                                + "; the unit is likely wrong — use [epoch_second] for unix epoch seconds"
                                + " or [epoch_millis] for datetime longs"
                        );
                        break;
                    }
                }
            }
        }
    }

    /**
     * {@code mo=month(ts)} on {@code {yyy}/{mo}} validates (the key is a placeholder) and then
     * {@code folderInterval} cannot see a year, so every folder is kept. Warn once. A hive folder
     * literally named {@code year} still satisfies the coarser grain via {@link #keyOf}.
     */
    private void emitMissingCoarserGrain(Set<String> detectedKeys, Consumer<String> sink) {
        for (List<Field> binds : temporalGroups().values()) {
            boolean anyDetected = false;
            int finestRank = -1;
            Field finestField = null;
            for (Field field : binds) {
                if (detectedKeys.contains(field.key())) {
                    anyDetected = true;
                }
                int rank = grainRank(field.transform());
                if (rank > finestRank) {
                    finestRank = rank;
                    finestField = field;
                }
            }
            if (anyDetected == false || finestField == null || finestRank <= 0) {
                continue;
            }
            for (Transform coarser : Transform.values()) {
                if (coarser.isTemporal() == false || grainRank(coarser) >= finestRank) {
                    continue;
                }
                if (coarserSatisfied(binds, coarser, detectedKeys)) {
                    continue;
                }
                sink.accept(
                    "["
                        + CONFIG_PARTITION_SPEC
                        + "] bind ["
                        + finestField.describe()
                        + "] needs a ["
                        + coarser.token()
                        + "] key to skip folders; add ["
                        + coarser.token()
                        + "("
                        + finestField.column()
                        + ")] or a path key named ["
                        + coarser.token()
                        + "]. The bind does not skip folders"
                );
            }
        }
    }

    private static boolean coarserSatisfied(List<Field> binds, Transform coarser, Set<String> detectedKeys) {
        for (Field field : binds) {
            if (field.transform() == coarser && detectedKeys.contains(field.key())) {
                return true;
            }
        }
        return detectedKeys.contains(coarser.token());
    }

    private Map<String, List<Field>> temporalGroups() {
        Map<String, List<Field>> groups = new LinkedHashMap<>();
        for (Field field : fields) {
            if (field.transform().isTemporal()) {
                groups.computeIfAbsent(field.column(), k -> new ArrayList<>()).add(field);
            }
        }
        return groups;
    }

    private List<Field> identityRemaps(String column, @Nullable Set<String> detectedKeys) {
        List<Field> remaps = new ArrayList<>(1);
        for (Field field : fields) {
            if (field.transform() == Transform.IDENTITY && field.column().equals(column) && field.key().equals(column) == false) {
                if (detectedKeys == null || detectedKeys.contains(field.key())) {
                    remaps.add(field);
                }
            }
        }
        return remaps;
    }

    private static List<Field> usable(List<Field> binds, @Nullable Set<String> detectedKeys) {
        if (detectedKeys == null) {
            return binds;
        }
        List<Field> kept = new ArrayList<>(binds.size());
        for (Field field : binds) {
            if (detectedKeys.contains(field.key())) {
                kept.add(field);
            }
        }
        return kept;
    }

    @Nullable
    private static Field coarsestYearBind(List<Field> binds) {
        for (Field field : binds) {
            if (field.transform() == Transform.YEAR) {
                return field;
            }
        }
        return null;
    }

    @Nullable
    private static Field finestBind(List<Field> binds, Map<String, Object> partitionValues) {
        Field finest = null;
        int finestRank = -1;
        for (Field field : binds) {
            int rank = grainRank(field.transform());
            if (rank > finestRank && intVal(partitionValues, field.key()) != null) {
                finest = field;
                finestRank = rank;
            }
        }
        return finest;
    }

    private static int grainRank(Transform transform) {
        return switch (transform) {
            case YEAR -> 0;
            case MONTH -> 1;
            case DAY -> 2;
            case HOUR -> 3;
            case IDENTITY -> -1;
        };
    }

    private static boolean hasHintOn(List<PartitionFilterHint> hints, String column) {
        for (PartitionFilterHint hint : hints) {
            if (hint.columnName().equals(column)) {
                return true;
            }
        }
        return false;
    }

    private static SourceBounds sourceBounds(List<PartitionFilterHint> hints, String column, Unit unit) {
        InstantRange range = InstantRange.ALL;
        Set<Long> equalsPoints = null;
        Set<Long> inPoints = null;
        for (PartitionFilterHint hint : hints) {
            if (hint.columnName().equals(column) == false) {
                continue;
            }
            switch (hint.operator()) {
                case EQUALS -> {
                    Long millis = toUtcMillisForProjection(hint.values().get(0), unit);
                    if (millis != null) {
                        equalsPoints = intersectPoint(equalsPoints, millis);
                    }
                }
                case IN -> {
                    Set<Long> next = new LinkedHashSet<>();
                    for (Object value : hint.values()) {
                        Long millis = toUtcMillisForProjection(value, unit);
                        if (millis != null) {
                            next.add(millis);
                        }
                    }
                    if (next.isEmpty() == false) {
                        inPoints = intersectSets(inPoints, next);
                    }
                }
                case GREATER_THAN -> {
                    Long millis = exclusiveLowerStartMillis(hint.values().get(0), unit);
                    if (millis != null) {
                        range = range.intersect(new InstantRange(millis, null));
                    }
                }
                case GREATER_THAN_OR_EQUAL -> {
                    Long millis = toUtcMillisForProjection(hint.values().get(0), unit);
                    if (millis != null) {
                        range = range.intersect(new InstantRange(millis, null));
                    }
                }
                case LESS_THAN -> {
                    Long millis = exclusiveUpperEndMillis(hint.values().get(0), unit);
                    if (millis != null) {
                        range = range.intersect(new InstantRange(null, millis));
                    }
                }
                case LESS_THAN_OR_EQUAL -> {
                    Long millis = toUtcMillisForProjection(hint.values().get(0), unit);
                    if (millis != null) {
                        range = range.intersect(new InstantRange(null, increment(millis)));
                    }
                }
                case NOT_EQUALS -> {
                    // A single excluded instant cannot drop a folder.
                }
            }
        }
        return new SourceBounds(range, combinePoints(equalsPoints, inPoints, range));
    }

    @Nullable
    private static Set<Long> intersectPoint(@Nullable Set<Long> existing, long point) {
        if (existing == null) {
            return Set.of(point);
        }
        return existing.contains(point) ? existing : Set.of();
    }

    @Nullable
    private static Set<Long> intersectSets(@Nullable Set<Long> existing, Set<Long> next) {
        if (existing == null) {
            return next;
        }
        Set<Long> kept = new LinkedHashSet<>(existing);
        kept.retainAll(next);
        return kept;
    }

    @Nullable
    private static Set<Long> combinePoints(@Nullable Set<Long> equalsPoints, @Nullable Set<Long> inPoints, InstantRange range) {
        Set<Long> points = null;
        if (equalsPoints != null && inPoints != null) {
            points = new LinkedHashSet<>(equalsPoints);
            points.retainAll(inPoints);
        } else if (equalsPoints != null) {
            points = equalsPoints;
        } else if (inPoints != null) {
            points = inPoints;
        }
        if (points == null) {
            return null;
        }
        Set<Long> filtered = new LinkedHashSet<>();
        for (Long point : points) {
            if (range.overlapsFolder(point, increment(point))) {
                filtered.add(point);
            }
        }
        return filtered;
    }

    private static List<Integer> overlappingYears(SourceBounds bounds) {
        if (bounds.points != null) {
            if (bounds.points.isEmpty()) {
                return List.of();
            }
            Set<Integer> years = new LinkedHashSet<>();
            for (Long point : bounds.points) {
                years.add(utcYear(point));
            }
            return List.copyOf(years);
        }
        if (bounds.range.isBounded() == false) {
            return List.of();
        }
        int startYear = utcYear(bounds.range.startInclusiveMillis);
        int endYear = utcYear(bounds.range.endExclusiveMillis - 1);
        int span = endYear - startYear + 1;
        if (span <= 0 || span > (WRONG_UNIT_YEAR_MAX - WRONG_UNIT_YEAR_MIN + 1)) {
            return List.of();
        }
        List<Integer> years = new ArrayList<>(span);
        for (int year = startYear; year <= endYear; year++) {
            years.add(year);
        }
        return years;
    }

    @Nullable
    private static long[] folderInterval(List<Field> binds, Map<String, Object> partitionValues, Transform grain) {
        Integer year = keyOf(binds, partitionValues, Transform.YEAR);
        Integer month = keyOf(binds, partitionValues, Transform.MONTH);
        Integer day = keyOf(binds, partitionValues, Transform.DAY);
        Integer hour = keyOf(binds, partitionValues, Transform.HOUR);
        try {
            return switch (grain) {
                case YEAR -> {
                    if (year == null) {
                        yield null;
                    }
                    Instant start = LocalDate.of(year, 1, 1).atStartOfDay().toInstant(ZoneOffset.UTC);
                    Instant end = LocalDate.of(year, 1, 1).plusYears(1).atStartOfDay().toInstant(ZoneOffset.UTC);
                    yield new long[] { start.toEpochMilli(), end.toEpochMilli() };
                }
                case MONTH -> {
                    if (year == null || month == null) {
                        yield null;
                    }
                    YearMonth ym = YearMonth.of(year, month);
                    yield new long[] {
                        ym.atDay(1).atStartOfDay().toInstant(ZoneOffset.UTC).toEpochMilli(),
                        ym.plusMonths(1).atDay(1).atStartOfDay().toInstant(ZoneOffset.UTC).toEpochMilli() };
                }
                case DAY -> {
                    if (year == null || month == null || day == null) {
                        yield null;
                    }
                    LocalDate date = LocalDate.of(year, month, day);
                    yield new long[] {
                        date.atStartOfDay().toInstant(ZoneOffset.UTC).toEpochMilli(),
                        date.plusDays(1).atStartOfDay().toInstant(ZoneOffset.UTC).toEpochMilli() };
                }
                case HOUR -> {
                    if (year == null || month == null || day == null || hour == null) {
                        yield null;
                    }
                    LocalDateTime hourStart = LocalDateTime.of(year, month, day, hour, 0);
                    yield new long[] {
                        hourStart.toInstant(ZoneOffset.UTC).toEpochMilli(),
                        hourStart.plusHours(1).toInstant(ZoneOffset.UTC).toEpochMilli() };
                }
                case IDENTITY -> null;
            };
        } catch (RuntimeException e) {
            return null;
        }
    }

    @Nullable
    private static Integer keyOf(List<Field> binds, Map<String, Object> partitionValues, Transform transform) {
        for (Field field : binds) {
            if (field.transform() == transform) {
                Integer value = intVal(partitionValues, field.key());
                if (value != null) {
                    return value;
                }
            }
        }
        return intVal(partitionValues, transform.token());
    }

    @Nullable
    static Integer intVal(Map<String, Object> values, String key) {
        Object raw = values.get(key);
        if (raw == null) {
            return null;
        }
        if (raw instanceof Number n) {
            return n.intValue();
        }
        try {
            return Integer.valueOf(BytesRefs.toString(raw));
        } catch (NumberFormatException e) {
            return null;
        }
    }

    @Nullable
    static Long toUtcMillis(Object raw, Unit unit) {
        if (raw == null) {
            return null;
        }
        Object value = raw instanceof BytesRef br ? BytesRefs.toString(br) : raw;
        if (value instanceof TemporalAccessor accessor) {
            return temporalToMillis(accessor);
        }
        if (value instanceof Number n) {
            return applyUnit(n.longValue(), unit);
        }
        if (value instanceof String s) {
            Long parsed = parseDateString(s);
            if (parsed != null) {
                return parsed;
            }
            try {
                return applyUnit(Long.parseLong(s), unit);
            } catch (NumberFormatException e) {
                return null;
            }
        }
        return null;
    }

    /**
     * Same conversion as {@link #toUtcMillis}, but a numeric bound whose
     * calendar year falls outside 1971–2100 is dropped so a wrong unit cannot
     * empty the scan. Datetime literals are unchanged (archives can be real).
     */
    @Nullable
    static Long toUtcMillisForProjection(Object raw, Unit unit) {
        Long millis = toUtcMillis(raw, unit);
        if (millis == null || isNumericLiteral(raw) == false) {
            return millis;
        }
        int year = utcYear(millis);
        if (year < WRONG_UNIT_YEAR_MIN || year > WRONG_UNIT_YEAR_MAX) {
            return null;
        }
        return millis;
    }

    @Nullable
    private static Long temporalToMillis(TemporalAccessor accessor) {
        return switch (accessor) {
            case Instant instant -> instant.toEpochMilli();
            case ZonedDateTime zdt -> zdt.toInstant().toEpochMilli();
            case OffsetDateTime odt -> odt.toInstant().toEpochMilli();
            case LocalDateTime ldt -> ldt.toInstant(ZoneOffset.UTC).toEpochMilli();
            case LocalDate date -> date.atStartOfDay().toInstant(ZoneOffset.UTC).toEpochMilli();
            default -> accessor.isSupported(ChronoField.INSTANT_SECONDS) ? Instant.from(accessor).toEpochMilli() : null;
        };
    }

    @Nullable
    private static Long parseDateString(String s) {
        try {
            return Instant.parse(s).toEpochMilli();
        } catch (DateTimeParseException ignored) {
            // try offset / local forms below
        }
        try {
            return OffsetDateTime.parse(s).toInstant().toEpochMilli();
        } catch (DateTimeParseException ignored) {
            // try local date-time below
        }
        try {
            return LocalDateTime.parse(s).toInstant(ZoneOffset.UTC).toEpochMilli();
        } catch (DateTimeParseException ignored) {
            // try date-only below
        }
        try {
            return LocalDate.parse(s).atStartOfDay().toInstant(ZoneOffset.UTC).toEpochMilli();
        } catch (DateTimeParseException e) {
            return null;
        }
    }

    @Nullable
    private static Long applyUnit(long numeric, Unit unit) {
        try {
            return switch (unit) {
                case EPOCH_SECOND -> Math.multiplyExact(numeric, 1000L);
                case EPOCH_MILLIS -> numeric;
            };
        } catch (ArithmeticException e) {
            return null;
        }
    }

    private static boolean isNumericLiteral(Object raw) {
        Object value = raw instanceof BytesRef br ? BytesRefs.toString(br) : raw;
        if (value instanceof Number) {
            return true;
        }
        if (value instanceof String s) {
            if (parseDateString(s) != null) {
                return false;
            }
            try {
                Long.parseLong(s);
                return true;
            } catch (NumberFormatException e) {
                return false;
            }
        }
        return false;
    }

    static int utcYear(long millis) {
        return Instant.ofEpochMilli(millis).atZone(ZoneOffset.UTC).get(ChronoField.YEAR);
    }

    /**
     * Exclusive {@code >} as an inclusive start. {@link Instant#toEpochMilli()} floors,
     * so a DATE_NANOS bound inside a millisecond would otherwise skip the rest of that
     * millisecond and drop a folder that still matches.
     */
    @Nullable
    private static Long exclusiveLowerStartMillis(Object raw, Unit unit) {
        Long millis = toUtcMillisForProjection(raw, unit);
        if (millis == null) {
            return null;
        }
        return hasSubMilliNanos(raw) ? millis : increment(millis);
    }

    /**
     * Exclusive {@code <} as an exclusive end. Sub-milli DATE_NANOS widens to the
     * containing millisecond so the boundary folder is kept.
     */
    @Nullable
    private static Long exclusiveUpperEndMillis(Object raw, Unit unit) {
        Long millis = toUtcMillisForProjection(raw, unit);
        if (millis == null) {
            return null;
        }
        return hasSubMilliNanos(raw) ? increment(millis) : millis;
    }

    private static boolean hasSubMilliNanos(Object raw) {
        return raw instanceof Instant instant && instant.getNano() % 1_000_000 != 0;
    }

    private static long increment(long millis) {
        return millis == Long.MAX_VALUE ? millis : millis + 1;
    }

    private record SourceBounds(InstantRange range, @Nullable Set<Long> points) {
        boolean overlaps(long folderStart, long folderEnd) {
            if (points != null) {
                if (points.isEmpty()) {
                    return false;
                }
                for (Long point : points) {
                    if (point >= folderStart && point < folderEnd) {
                        return true;
                    }
                }
                return false;
            }
            return range.overlapsFolder(folderStart, folderEnd);
        }
    }

    @Override
    public boolean equals(Object o) {
        return o instanceof PartitionSpec other && fields.equals(other.fields);
    }

    @Override
    public int hashCode() {
        return fields.hashCode();
    }

    @Override
    public String toString() {
        return "PartitionSpec" + fields;
    }
}
